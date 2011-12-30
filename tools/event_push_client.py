# -*- coding: utf-8 -*-
"""
event_push_client

class EventPushClient

a PUSH client with utility functions for event notifications.
"""
import os
import sys
import time

from tools.push_client import PUSHClient

unhandled_exception_topic = "unhandled_exception"

_level_debug = "debug"
_level_info = "info"
_level_warn = "warn"
_level_error = "error"
_level_exception = "exception"

_level_rank = {
    _level_debug    : 1,
    _level_info     : 2,
    _level_warn     : 3,
    _level_error    : 4,
    _level_exception: 5,
}

def level_cmp(lhs, rhs):
    """
    return the difference between the ranks of two levels
    < 0 == lhs < rhs
    = 0 == lhs = rhs
    > 0 == lhs > rhs
    """
    return _level_rank[lhs] - _level_rank[rhs]

def exception_event(state):
    """
    an exception reporting callback for time queue processes
    we assume this is being called where the exception is being handled
    """
    exctype, value = sys.exc_info()[:2]
    state["event-push-client"].exception(
        unhandled_exception_topic,
        str(value),
        exctype=exctype.__name__
    )

class EventPushClient(PUSHClient):
    """
    a PUSH client with utility functions for event notifications.
    """
    def __init__(self, context, source_name):
        super(EventPushClient, self).__init__(
            context, os.environ["NIMBUSIO_EVENT_PUBLISHER_PULL_ADDRESS"]
        )
        self._source_name = source_name
    
    def info(self, event, description, **kwargs):
        """
        an informational event
        """
        kwargs["level"] = _level_info
        self.send_event(event, description, **kwargs)

    def warn(self, event, description, **kwargs):
        """
        a warning event
        """
        kwargs["level"] = _level_warn
        self.send_event(event, description, **kwargs)

    def error(self, event, description, **kwargs):
        """
        an error event
        """
        kwargs["level"] = _level_error
        self.send_event(event, description, **kwargs)

    def exception(self, event, description, **kwargs):
        """
        an error event
        """
        kwargs["level"] = _level_exception
        self.send_event(event, description, **kwargs)

    def send_event(self, event, description, **kwargs):
        message = {
            "message-type"  : event,
            "source"        : self._source_name,
            "description"   : description,            
            "timestamp"     : time.time(),
        }
        message.update(kwargs)
        self.send(message)

