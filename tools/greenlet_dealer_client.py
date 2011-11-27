# -*- coding: utf-8 -*-
"""
greenlet_dealer_client.py

a class that manages a zeromq DEALER (aka XREQ) socket as a client,
to a ROUTER (aka XREP) server
"""
import logging
import uuid

from gevent.queue import Queue
from gevent_zeromq import zmq

from tools.data_definitions import message_format

class GreenletDealerClient(object):
    """
    a class that manages a zeromq DEALER (aka XREQ) socket as a client
    """
    def __init__(self, context, node_name, address):
        self._log = logging.getLogger("DealerClient-%s" % (node_name, ))

        self._dealer_socket = context.socket(zmq.XREQ)
        self._dealer_socket.setsockopt(zmq.LINGER, 1000)
        self._log.debug("connecting to %s" % (address, ))
        self._dealer_socket.connect(address)

        self._send_queue = Queue(maxsize=None)
        self._delivery_queues = dict()

    def register(self, pollster):
        pollster.register_read_or_write(
            self._dealer_socket, self._pollster_callback
        )

    def unregister(self, pollster):
        pollster.unregister(self._dealer_socket)

    def close(self):
        self._dealer_socket.close()

    def queue_message_for_send(self, message_control, data=None):
        """
        message control must be a dict 
        If the caller includes a 'message-id' key we will use it,
        otherwise, we will supply one.
        return: a gevent queue (zero size Queue) the the reply will
        be deliverd.
        """
        if not "message-id" in message_control:
            message_control["message-id"] = uuid.uuid1().hex
        self._delivery_queues[message_control["message-id"]] = \
            Queue(maxsize=None)
        self._send_queue.put(
            message_format(ident=None, control=message_control, body=data)
        )
        return self._delivery_queues[message_control["message-id"]]

    def _pollster_callback(self, _active_socket, readable, writable):
        # push our output first
        if writable:
            while not self._send_queue.empty():
                message = self._send_queue.get_nowait()
                self._send_message(message)

        # if we have input, read it and queue it
        if readable:
            message = self._receive_message()      
            # if we get None, that means the socket would have blocked
            # go back and wait for more
            if message is None:
                return None
            if not "message-id" in message.control:
                self._log.error("message has no 'message-id' %s" % (
                    message.control
                ))
            else:
                try:
                    delivery_queue = self._delivery_queues.pop(
                        message.control["message-id"]
                    )
                except KeyError:
                    self._log.error("No delivery queue for %s" % (
                        message.control, 
                    ))
                else:
                    delivery_queue.put((message.control, message.body, ))

    def _send_message(self, message):
        self._log.info("sending message: %s" % (message.control, ))

        # don't send a zero size body 
        if type(message.body) not in [list, tuple, type(None), ]:
            if len(message.body) == 0:
                message = message._replace(body=None)
            else:
                message = message._replace(body=[message.body, ])

        if message.body is None:
            self._dealer_socket.send_json(message.control)
        else:
            self._dealer_socket.send_json(message.control, zmq.SNDMORE)
            for segment in message.body[:-1]:
                self._dealer_socket.send(segment, zmq.SNDMORE)
            self._dealer_socket.send(message.body[-1])

    def _receive_message(self):
        try:
            control = self._dealer_socket.recv_json(zmq.NOBLOCK)
        except zmq.ZMQError, instance:
            if instance.errno == zmq.EAGAIN:
                self._log.warn("socket would have blocked")
                return None
            raise

        body = []
        while self._dealer_socket.rcvmore:
            body.append(self._dealer_socket.recv())

        # 2011-04-06 dougfort -- if someone is expecting a list and we only get
        # one segment, they are going to have to deal with it.
        if len(body) == 0:
            body = None
        elif len(body) == 1:
            body = body[0]

        return message_format(ident=None, control=control, body=body)

