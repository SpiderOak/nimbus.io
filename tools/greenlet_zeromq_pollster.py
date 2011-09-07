# -*- coding: utf-8 -*-
"""
greenlet_zeromq_pollster.py

An object to poll zeromq sockets
"""
import logging

import gevent
from gevent_zeromq import zmq

class ZeroMQPollsterError(Exception):
    pass

class GreenletZeroMQPollster(gevent.Greenlet):
    """encapsulate gevent_zeromq.zmq.poller"""
    
    def __init__(self, polling_interval=0.1, poll_timeout=0.1):
        self._log = logging.getLogger("pollster")
        self._polling_interval = polling_interval
        self._poll_timeout = poll_timeout
        self._poller = zmq.Poller()
        self._active_sockets = dict()
        gevent.Greenlet.__init__(self)
        
    def register_read(self, active_socket, callback):
        """register a socket for reading, with a callback function"""
        self.unregister(active_socket)
        self._poller.register(active_socket, zmq.POLLIN)
        self._active_sockets[active_socket] = callback

    def register_write(self, active_socket, callback):
        """register a socket for writing, with a callback function"""
        self.unregister(active_socket)
        self._poller.register(active_socket, zmq.POLLOUT)
        self._active_sockets[active_socket] = callback

    def register_read_or_write(self, active_socket, callback):
        """register a socket for reading or writing with a callback function"""
        self.unregister(active_socket)
        self._poller.register(active_socket, zmq.POLLIN | zmq.POLLOUT)
        self._active_sockets[active_socket] = callback 

    def unregister(self, active_socket):
        """remove from poll. Don't fail if already gone"""
        try:
            self._poller.unregister(active_socket)
            del self._active_sockets[active_socket]
        except KeyError:
            pass

    def _run(self):
        """
        poll for I/O ready objects
        """
        while True:
            for active_socket, event_flags in self._poller.poll(
                timeout=self._poll_timeout
            ):
                if active_socket not in self._active_sockets:
                    self._log.warn("Ignoring unknown active_socket %s" % (
                        active_socket,
                    )) 
                    self._poller.unregister(active_socket)
                    continue

                if event_flags & zmq.POLLERR:
                    message = ("Error flag from poll() %s" % (
                        active_socket,
                    )) 
                    self._log.error(message)
                    raise ZeroMQPollsterError(message)

                callback = self._active_sockets[active_socket]

                readable = (True if event_flags & zmq.POLLIN else False)  
                writable = (True if event_flags & zmq.POLLOUT else False)  
           
                callback(active_socket, readable=readable, writable=writable)
                
            gevent.sleep(self._polling_interval)

