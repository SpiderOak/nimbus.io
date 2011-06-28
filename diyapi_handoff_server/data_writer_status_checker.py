# -*- coding: utf-8 -*-
"""
data_writer_status_checker.py

class DataWriterStatusChecker

A time queue action to periodically check the status of data writers.
If a data writer becomes connected, we will hand off the actions we are
holding for it.
"""
from collections import defaultdict
import logging
import os
import time

from diyapi_handoff_server.forwarder_coroutine import forwarder_coroutine

_node_names = os.environ['SPIDEROAK_MULTI_NODE_NAME_SEQ'].split()

_polling_interval = 15.0
_reporting_interval = 60.0

# we don't want to jump on a data writer as soon as it connects
# we want it to see it stay up for a while before we start handing off
_connected_test_time = 3 * _polling_interval

# even if we think we're up to date, # we may have missed a brief outage
_up_to_date_test_time = 15 * 60.0

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
        self._last_report_time = 0.0

    @classmethod
    def next_run(cls):
        return time.time() + _polling_interval

    def run(self, halt_event):
        if halt_event.is_set():
            return

        try:
            self._current_time_check()
        except Exception, instance:
            self._log.exception(instance)
        
        return [(self.run, self.next_run(), )]

    def _current_time_check(self):
        current_time = time.time()
        for node_name, status_entry in self._node_status.items():
            # we are looking for data writers who have connected recently
            # and who have stayed connected for a while
            if status_entry["handoff-status"] == "disconnected":
                if status_entry["writer-client"].connected:
                    self._log.info("marking node %s 'connected'" % (
                        node_name, 
                    ))
                    status_entry["handoff-status"] = "connected"
                    status_entry["status-time"] = current_time
            else:
                if status_entry["writer-client"].connected:
                    elapsed_time = current_time - status_entry["status-time"]
                    if status_entry["handoff-status"] == "connected":
                        if elapsed_time >= _connected_test_time:
                            self._check_for_hint(current_time, status_entry)
                    elif status_entry["handoff-status"] == "up-to-date":
                        if elapsed_time >= _up_to_date_test_time:
                            self._check_for_hint(current_time, status_entry)
                else:
                    self._log.info("marking node %s 'disconnected'" % (
                        node_name, 
                    ))
                    status_entry["handoff-status"] = "disconnected"
                    status_entry["status-time"] = current_time

        elapsed_time = current_time - self._last_report_time
        if elapsed_time < _reporting_interval:
            return
        
        status_dict = defaultdict(lambda : 0)
        for node_name, status_entry in self._node_status.items():
            status_dict[status_entry["handoff-status"]] += 1
        result = "; ".join(["%s %s" % (k, v,) for k, v in status_dict.items()])
        self._log.info(result)

        self._last_report_time = current_time

    def check_node_for_hint(self, node_name):
        """
        see if we have a pending hint for the given node
        set the status accordingly
        """
        self._log.debug("check_node_for_hint: %s" % (node_name, ))
        current_time = time.time()
        status_entry = self._node_status[node_name]
        assert status_entry["handoff-status"] in [
            "connected", "up-to-data", "in-progress"
        ], str(status_entry)

        if not status_entry["writer-client"].connected:
            self._log.info("marking node %s 'disconnected'" % ( node_name, ))
            status_entry["handoff-status"] = "disconnected"
            status_entry["status-time"] = current_time
        else:
            self._check_for_hint(current_time, status_entry)

    def _check_for_hint(self, current_time, status_entry):
        self._log.debug("_check_for_hint: %s" % (status_entry["node-name"], ))
        hint = self._state["hint-repository"].next_hint(
            status_entry["node-name"]
        )
        if hint is None:
            status_entry["handoff-status"] = "up-to-date"
            status_entry["status-time"] = current_time
        else:
            if self._start_handoff(hint, status_entry["writer-client"]):
                status_entry["handoff-status"] = "in-progress"
            status_entry["status-time"] = current_time

    def _start_handoff(self, hint, writer_client):
        self._log.info("starting handoffs to %s" % (hint.node_name, ))
        assert hint.node_name not in self._state["active-forwarders"] 

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
        self._state["active-forwarders"][message_id] = forwarder

        return True
