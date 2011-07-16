# -*- coding: utf-8 -*-
"""
message_handler.py

class MessageHandler

A greenlet object to manage messages from the receive_queue
"""
import logging

import gevent
from gevent_zeromq import zmq
from  gevent.greenlet import Greenlet

from sample_code.diy_client.archiver import archive_blob, archive_file

class MessageHandler(Greenlet):
    """
    A greenlet object to manage messages from the receive_queue
    """
    def __init__(
        self, halt_event, config, context, send_queue, receive_queue
    ):
        Greenlet.__init__(self)
        self._log = logging.getLogger("MessageHandler")
        self._halt_event = halt_event
        self._config = config
        self._send_queue = send_queue
        self._receive_queue = receive_queue
        self._dispatch_table = {
            "archive-blob" : archive_blob,
            "archive-file" : archive_file,
        }

    def join(self, timeout=None):
        """
        close the _pub socket
        """
        Greenlet.join(self, timeout)

    def _run(self):
        while not self._halt_event.is_set():
            message, body = self._receive_queue.get()
            self._log.info(str(message))

            try:
                handler = self._dispatch_table[message["message-type"]]
            except KeyError:
                self._log.error("Unknown message-type %s" % (message, ))
            else:
                gevent.spawn(
                    handler, 
                    self._config, message, body, self._send_queue
                )

