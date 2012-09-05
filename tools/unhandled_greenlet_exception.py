# -*- coding: utf-8 -*-
"""
unhandled_greenlet_exception.py

a closure that returns a function suitable for greenlet.link_exception 
"""
import logging

from tools.event_push_client import unhandled_exception_topic

def unhandled_greenlet_exception_closure(event_push_client):
    def _report_exception(greenlet_object):
        log = logging.getLogger("unhandled_greenlet_exception")
        error_message = "{0} {1} {2}".format(
            str(greenlet_object),
            greenlet_object.exception.__class__.__name__,
            str(greenlet_object.exception))
        log.error(error_message)
        event_push_client.exception(unhandled_exception_topic, error_message)
    return _report_exception

