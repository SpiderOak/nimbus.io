# -*- coding: utf-8 -*-
"""
pinger.py

A Greenlet that sends a ping request to a specified url
it reports the results in the redis queue
"""
import logging
import re
import time

import gevent
import gevent.greenlet
import gevent.queue

import requests
from requests.exceptions import RequestException


_method_dispatch_table = {
    "get" : requests.get,
    "post" : requests.get,
    "put" : requests.put,
    "head" : requests.head,
    "delete" : requests.delete,
    "options" : requests.options,
}

class Pinger(gevent.greenlet.Greenlet):
    """
    A Greenlet that sends a ping request to a specified url
    it reports the results in the redis queue
    """
    def __init__(self, halt_event, polling_interval, redis_queue, config_entry):
        gevent.greenlet.Greenlet.__init__(self)
        self._halt_event = halt_event
        self._polling_interval = polling_interval
        self._redis_queue = redis_queue

        self._key = ":".join([config_entry["address"], 
                              str(config_entry["port"])])
        self._log = logging.getLogger("pinger_{0}".format(self._key))

        path = config_entry["path"]
        path = (path[1:] if path[0] == "/" else path)
        self._url = "http://{0}:{1}/{2}".format(config_entry["address"],
                                               config_entry["port"],
                                               path)

        # if we've got a bad method, let's blow up here, before we start the
        # greenlet
        self._method = _method_dispatch_table[config_entry["method"]]

        self._host = config_entry["host"]
        self._expected_status = config_entry["expected-status"]

        # if the re doesn't compile, lets' blow up here
        self._body_test = re.compile(config_entry["body-test"])

        self._timeout_seconds = config_entry["timeout-seconds"]

        self._prev_status = None 

    def join(self, timeout=None):
        self._log.info("joining")
        gevent.greenlet.Greenlet.join(self, timeout)

    def _run(self):
        self._log.info("pinging {0} {1} every {2} seconds".format(
            self._host, self._url, self._polling_interval)) 

        self._log.debug("start halt_event loop")
        while not self._halt_event.is_set():

            status = "ok"
            response = None
            with gevent.Timeout(self._timeout_seconds, False):
                try:
                    response = self._method(self._url)
                except RequestException, instance:
                    status = "request exception {0} {1}".format(
                        instance.__class__.__name__, instance)
                # request doesn't handle urllib3 errors
                except Exception, instance:
                    status = "exception {0} {1}".format(
                        instance.__class__.__name__, instance)
            
            if status == "ok":
                if response is None:
                    status = "timeout"
                elif response.status_code != self._expected_status:
                    status = "status_code error: expected {0} got {1}".format(
                            response.status_code, self._expected_status)
                elif self._body_test.match(response.text) is None:
                    status = "unmatched body {0}".format(response.text)

            if status != self._prev_status:
                self._log.info("status changes from {0} to {1}".format(
                    self._prev_status, status))

                status_dict = {
                    "reachable" : status == "ok",
                    "timestamp" : time.time()
                }

                self._redis_queue.put( (self._key, status_dict, ) )
                self._prev_status = status

            self._halt_event.wait(self._polling_interval)

        self._log.debug("end halt_event loop")


