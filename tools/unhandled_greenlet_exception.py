# -*- coding: utf-8 -*-
"""
unhandled_greenlet_exception.py

a closure that returns a function suitable for greenlet.link_exception 
"""
import logging

from tools.event_push_client import unhandled_exception_topic

def unhandled_greenlet_exception_closure(event_push_client):
    def _report_exception(greenlet_object):
        log = logging.getLogger("unhandled_greelent_exception")
        try:
            result = greenlet_object.get()
        except Exception, instance:
            error_message = "{0} {1}".format(instance.__class__.__name__,
                                             str(instance))
            log.exception(error_message)
            event_push_client.exception(unhandled_exception_topic,
                                        error_message)
        else:
            log.error("unexpected reault {0}".format(result))
    return _report_exception

