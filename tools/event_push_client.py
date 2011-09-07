# -*- coding: utf-8 -*-
"""
event_push_client

class EventPushClient

a PUSH client with utility functions for event notifications.
"""
import os
import sys

from tools.push_client import PUSHClient

_event_publisher_pull_address = \
        os.environ["NIMBUSIO_EVENT_PUBLISHER_PULL_ADDRESS"]

def exception_event(state):
    """
    an exception reporting callback for time queue processes
    we assume this is being called where the exception is being handled
    """
    exctype, value = sys.exc_info()[:2]
    state["event-push-client"].exception(
        "unhandled_exception",
        value,
        exctype=exctype.__name__
    )

class EventPushClient(PUSHClient):
    """
    a PUSH client with utility functions for event notifications.
    """
    def __init__(self, context, source_name):
        super(EventPushClient, self).__init__(
            context, _event_publisher_pull_address
        )
        self._source_name = source_name
    
    def info(self, event, description, **kwargs):
        """
        an informational event
        """
        kwargs["level"] = "info"
        self.send_event(event, description, **kwargs)

    def warn(self, event, description, **kwargs):
        """
        a warning event
        """
        kwargs["level"] = "warn"
        self.send_event(event, description, **kwargs)

    def error(self, event, description, **kwargs):
        """
        an error event
        """
        kwargs["level"] = "error"
        self.send_event(event, description, **kwargs)

    def exception(self, event, description, **kwargs):
        """
        an error event
        """
        kwargs["level"] = "exception"
        self.send_event(event, description, **kwargs)

    def send_event(self, event, description, **kwargs):
        message = {
            "message-type"  : event,
            "source"        : self._source_name,
            "description"   : description,            
        }
        message.update(kwargs)
        self.send(message)

