# -*- coding: utf-8 -*-
"""
data_writer_status_checker.py

class DataWriterStatusChecker

A time queue action to periodically check the status of data writers.
If a data writer becomes connected, we will hand off the actions we are
holding for it.
"""
import logging
import os
import time

from diyapi_handoff_server.forwarder_coroutine import forwarder_coroutine

_node_names = os.environ['SPIDEROAK_MULTI_NODE_NAME_SEQ'].split()

_polling_interval = 15.0

# we don't want to jump on a data writer as soo as it connects
# we want it to see it stay up for a while before we start handing off
_connected_test_time = 3 * _polling_interval

class DataWriterStatusChecker(object):
    """
    A time queue action to periodically check the status of data writers.
    If a data writer becomes connected, we will hand off the actions we are
    holding for it.
    """
    def __init__(self, state):
        self._log = logging.getLogger("DataWriterStatusChecker")
        self._state = state
        self._node_status = dict()
        for node_name, writer_client in zip(
            _node_names, state["writer-clients"]
        ):
            # we start out treating everything as disconnected
            self._node_status[node_name] = {
                "node-name"         : node_name,
                "writer-client"     : writer_client,
                "handoff-status"    : "disconnected",
                "status-time"       : 0.0,       
            }
        self._reader_clients_by_node_name = dict(
            zip(_node_names, state["reader-clients"])
        )

    @classmethod
    def next_run(cls):
        return time.time() + _polling_interval

    def run(self, halt_event):
        if halt_event.is_set():
            return
                
        current_time = time.time()
        for status_entry in self._node_status.values():
            # we are looking for data writers who have connected recently
            # and who have stayed connected for a while
            if status_entry["handoff-status"] == "disconnected":
                if status_entry["writer-client"].connected:
                    status_entry["handoff-status"] = "connected"
                    status_entry["status-time"] = current_time
            elif status_entry["handoff-status"] == "connected":
                elapsed_time = current_time - status_entry["status-time"]
                if elapsed_time >= _connected_test_time:
                    hint = self._state["hint-repository"].next_hint(
                        status_entry["node-name"]
                    )
                    if hint is None:
                        status_entry["handoff-status"] = "up-to-date"
                        status_entry["status-time"] = current_time
                    else:
                        if self._start_handoff(
                            hint, 
                            status_entry["writer-client"]
                        ):
                            status_entry["handoff-status"] = "in-progress"
                            status_entry["status-time"] = current_time

        return [(self.run, self.next_run(), )]

    def _start_handoff(self, hint, writer_client):
        self._log.info("starting handoffs to %s" % (hint.node_name, ))
        assert hint.node_name not in self._state["active-handoffs"] 

        server_node_names = hint.server_node_names.split()

        reader_client = None
        for server_node_name in server_node_names:
            work_client = self._reader_clients_by_node_name[server_node_name]
            if work_client.connected:
                reader_client = work_client
                break
        if reader_client is None:
            self._log.warn("No active reader clients for hint %s" % (hint, ))
            return False

        # when we are done with a handoff, we want to purge the key
        # from the nodes where it was backed up
        purge_writer_clients = list()
        for server_node_name in server_node_names:
            client = self._node_status[server_node_name]["writer-client"]
            if client.connected:
                purge_writer_clients.append(client)

        forwarder = forwarder_coroutine(
            hint, writer_client, reader_client, purge_writer_clients
        )
        message_id = forwarder.next()
        self._state["active-forwarders"][message_id]
