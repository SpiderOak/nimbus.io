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

_node_names = os.environ['SPIDEROAK_MULTI_NODE_NAME_SEQ'].split()

_polling_interval = 15.0

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
            self._node_status[node_name] = {
                "writer-client" : writer_client,
                "connected"     : False,
                "status-time"   : 0.0,       
            }

    @classmethod
    def next_run(cls):
        return time.time() + _polling_interval

    def run(self, halt_event):
        if halt_event.is_set():
            return
                
        current_time = time.time()
        for node_name, status_entry in self._node_status.items():



        return [(self.run, self.next_run(), )]

def _check_for_handoffs(state, dest_exchange):
    """
    initiate the the process of retrieving handoffs and sending them to
    the data_writer at the destination_exchange
    """
    log = logging.getLogger("_start_returning_handoffs")
    hint = state["hint-repository"].next_hint(dest_exchange)
    if hint is None:
        return []
    request_id = uuid.uuid1().hex
    log.debug("found hint for exchange = %s assigning request_id %s" % (
        dest_exchange, request_id,  
    ))
    state[request_id] = forwarder_coroutine(request_id, hint, _routing_header) 
    return state[request_id].next()

