# -*- coding: utf-8 -*-
"""
pinger.py

A Greenlet that sends a ping request to a specified url
it reports the results in the redis queue
"""
import httplib
import logging
import re
import time
import socket

import gevent
import gevent.greenlet
import gevent.queue

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

        self._path = config_entry["path"]
        self._address = config_entry["address"]
        self._port = int(config_entry["port"])

        self._method = config_entry["method"].upper()

        self._host = config_entry["host"]
        self._expected_status = config_entry["expected-status"]

        # if the re doesn't compile, lets' blow up here
        self._body_test = re.compile(config_entry["body-test"])

        self._timeout_seconds = config_entry["timeout-seconds"]

        self._prev_status = None 

    def join(self, timeout=None):
        self._log.info("joining")
        gevent.greenlet.Greenlet.join(self, timeout)

    def _ping(self):
        status = "ok"
        response = None
        connection = httplib.HTTPConnection(self._address, 
                                            self._port)
        with gevent.Timeout(self._timeout_seconds, False):

            try:
                connection.request(self._method, self._path)
                response = connection.getresponse()
                body = response.read()
            except httplib.HTTPException, instance:
                status = "HTTPException {0}".format(instance.message)
            except socket.error, instance:
                status = "socket error {0}".format(instance.message)
            except Exception, instance:
                self._log.exception("_ping {0}".format(type(instance)))
                status = "{0} {1}".format(instance.__class__.__name__,
                                          instance)
        connection.close()

        if status == "ok":
            if response is None:
                status = "timeout"
            elif response.status != self._expected_status:
                status = "status_code error: expected {0} got {1}".format(
                        response.status, self._expected_status)
            elif self._body_test.match(body) is None:
                status = "unmatched body '{0}'".format(body)

        if status != self._prev_status:
            self._log.info("status changes from {0} to {1}".format(
                self._prev_status, status))

            status_dict = {
                "reachable" : status == "ok",
                "timestamp" : time.time()
            }

            self._redis_queue.put( (self._key, status_dict, ) )
            self._prev_status = status

    def _run(self):
        self._log.info("pinging {0} {1}:{2} every {3} seconds".format(
            self._host, self._address, self._port, self._polling_interval)) 

        self._log.debug("start halt_event loop")
        while not self._halt_event.is_set():

            try:
                self._ping()
            except Exception, instance:
                self._log.exception("_run {0}".format(type(instance)))

            self._halt_event.wait(self._polling_interval)

        self._log.debug("end halt_event loop")


