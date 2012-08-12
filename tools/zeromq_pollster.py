# -*- coding: utf-8 -*-
"""
zeromq_pollster.py

A time_queue object to poll zeromq sockets
"""
import logging
import time

import zmq

from tools.zeromq_util import is_interrupted_system_call 

class ZeroMQPollsterError(Exception):
    pass

class ZeroMQPollster(object):
    """
    This is a class that encapsulates the zeromq `poller`_.
    The ``run`` member function is a callback for the time queue event loop.

    polling_interval (optional)
        How often the poller is called from the event loop

    poll_timout (optional)
        How long the poller waits for a zeromq socket to be available

    This class maintains a dictionary of active sockets with associatd callback
    functions. When the pollster finds that a socket is ready for non-blocking 
    I/O, it calls the callback.

    Note that zeromq sockets are almost always writable so the pollster
    is used mostly for reads.
        
    .. _poller: http://zeromq.github.com/pyzmq/api/generated/zmq.core.poll.html
    """
    
    def __init__(self, polling_interval=0.1, poll_timeout=1000):
        self._log = logging.getLogger("pollster")
        self._polling_interval = polling_interval
        self._poll_timeout = poll_timeout
        self._poller = zmq.Poller()
        self._active_sockets = dict()
        
    def register_read(self, active_socket, callback):
        """
        register a socket for reading, with a callback function
        """
        self.unregister(active_socket)
        self._poller.register(active_socket, zmq.POLLIN)
        self._active_sockets[active_socket] = callback

    def register_write(self, active_socket, callback):
        """
        register a socket for writing, with a callback function
        """
        self.unregister(active_socket)
        self._poller.register(active_socket, zmq.POLLOUT)
        self._active_sockets[active_socket] = callback

    def register_read_or_write(self, active_socket, callback):
        """
        register a socket for reading or writing with a callback function
        """
        self.unregister(active_socket)
        self._poller.register(active_socket, zmq.POLLIN | zmq.POLLOUT)
        self._active_sockets[active_socket] = callback 

    def unregister(self, active_socket):
        """
        remove from poll. Don't fail if already gone
        """
        try:
            self._poller.unregister(active_socket)
            del self._active_sockets[active_socket]
        except KeyError:
            pass

    def run(self, halt_event):
        """
        poll for I/O ready objects
        This is a task for the time queue
        """
        
        if halt_event.is_set():
            self._log.info("halt_event set: exiting")
            for active_socket in self._active_sockets.keys():
                self._poller.unregister(active_socket)
            self._active_sockets.clear()
            return
        
        next_tasks = list()
        next_interval = time.time() + self._polling_interval

        try:
            result_list = self._poller.poll(timeout=self._poll_timeout)
        except zmq.ZMQError, zmq_error:
            if is_interrupted_system_call(zmq_error) and halt_event.is_set():
                self._log.info("Interrupted with halt_event set: exiting")
                for active_socket in self._active_sockets.keys():
                    self._poller.unregister(active_socket)
                self._active_sockets.clear()
                return
            raise

        for active_socket, event_flags in result_list:
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
       
            result_list = callback(
                active_socket,
                readable=readable,  
                writable=writable,  
            )
            if result_list is not None:
                next_tasks.extend(result_list)
                        
            # 2010-10-25 dougfort -- If the socket was readable, we probably 
            # picked up new work. So go back and poll again right away.
            # These zmq sockets seem to pretty well always be writable,
            # so don't assume we actually did anything.
            if readable:
                next_interval = time.time()

        # put ourselves into the time queue to run at the next polling interval
        next_tasks.append((self.run, next_interval, ))
        return next_tasks
            
