# -*- coding: utf-8 -*-
"""
low_traffic_thread.py

A thread which sends a 'low_traffic' message if it doesn't get reset.
This breaks the pyaqmp socket it of its blocking read, so we can run
timeout checks, etc.
"""
from threading import Thread
import time

import amqplib.client_0_8 as amqp

from tools import amqp_connection

low_traffic_routing_tag = "low_traffic"
_timeout_interval = 60.0 

class LowTrafficThread(Thread):
    """A thread which sends a 'low_traffic' message if it doesn't get reset."""
    def __init__(self, halt_event, connection, routing_header):
        Thread.__init__(self)

        self._halt_event = halt_event
        self._connection = connection

        self._routing_key = ".".join([routing_header, low_traffic_routing_tag])

        self._timeout = None
        self.reset()

    def run(self):
        """
        sleep for the timeout interval, 
        if the timeout has not been reset, send a low_traffic message
        """
        while not self._halt_event.is_set():
            self._halt_event.wait(_timeout_interval)
            if time.time() > self._timeout:
                self._send_timeout_message()
            self.reset()

    def reset(self):
        """reset the timeout to start from the current time"""
        self._timeout = time.time() + _timeout_interval

    def _send_timeout_message(self):
        channel = self._connection.channel()
        amqp_message = amqp.Message("low traffic")
        channel.basic_publish( 
            amqp_message, 
            exchange=amqp_connection.local_exchange_name, 
            routing_key=self._routing_key,
            mandatory = True
        )
        channel.close()

