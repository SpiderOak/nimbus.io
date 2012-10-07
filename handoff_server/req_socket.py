# -*- coding: utf-8 -*-
"""
req_socket.py

wrap a zeromq REQ socket, with check for ack
"""
import logging
import time
import uuid

from gevent_zeromq import zmq

from tools.zeromq_util import prepare_ipc_path
from tools.data_definitions import message_format

class ReqSocketError(Exception):
    pass
class ReqSocketReplyTimeOut(ReqSocketError):
    pass
class ReqSocketAckTimeOut(ReqSocketReplyTimeOut):
    pass

_reply_timeout_seconds = 120.0
_ack_timeout_seconds = 120.0

class ReqSocket(object):
    """
    wrap a zeromq REQ socket, with check for ack
    """
    def __init__(self, 
                 zmq_context, 
                 address, 
                 client_tag, 
                 client_address, 
                 halt_event):
        self._name = address
        self._log = logging.getLogger(self._name)

        self._socket = zmq_context.socket(zmq.REQ)

        # we need a valid path for IPC sockets
        if address.startswith("ipc://"):
            prepare_ipc_path(address)

        self._log.info("connecting to {0}".format(address))
        self._socket.connect(address)

        self._client_tag = client_tag
        self._client_address = client_address
        self._halt_event = halt_event

    def __str__(self):
        return self._name

    def close(self):
        self._socket.close()

    def send(self, message, data=None):
        """
        send a message immediately to zeromq 

        message
            a dict, suitable for transmission as JSON
            if message does not include 'message-id', this function will supply
            it

        data
            either None, or a sequence of binary data segments
        """
        if not "message-id" in message:
            message["message-id"] = uuid.uuid1().hex
        if not "client-tag" in message:
            message["client-tag"] = self._client_tag
        if not "client-address" in message:
            message["client-address"] = self._client_address
        
        if data is None:
            self._socket.send_json(message)
        else:
            self._socket.send_json(message, zmq.SNDMORE)
            for data_segment in data[:-1]:
                self._socket.send(data_segment, zmq.SNDMORE)
            self._socket.send(data[-1])

    def wait_for_reply(self, timeout=_reply_timeout_seconds):
        """
        return when ack is received
        raise ReqSocketReplyTimeout if reply is not received
        """
        # 2012-09-06 dougfort -- gevent.Timeout goes off into outer space here
        start_time = time.time()
        while not self._halt_event.is_set():
            try:
                control = self._socket.recv_json(zmq.NOBLOCK)
            except zmq.ZMQError, instance:
                if instance.errno == zmq.EAGAIN:
                    elapsed_time = time.time() - start_time
                    if elapsed_time < timeout:
                        self._halt_event.wait(1.0)
                        continue
                    self.close()
                    error_message = "Timout waiting reply {0} seconds".format(
                        timeout)
                    self._log.error(error_message)
                    raise ReqSocketReplyTimeOut(error_message)
                raise
            else:
                break

        body = []
        while self._socket.rcvmore:
            body.append(self._socket.recv())

        # 2011-04-06 dougfort -- if someone is expecting a list and we 
        # only get one segment, they are going to have to deal with it.
        if len(body) == 0:
            body = None
        elif len(body) == 1:
            body = body[0]
        self._log.debug("received: {0}".format(control))
        return message_format(ident=None, control=control, body=body)

    def wait_for_ack(self, timeout=_ack_timeout_seconds):
        """
        return when ack is received
        raise ReqSocketAckTimeout if ack is not received
        """
        try:
            message = self.wait_for_reply(timeout)
        except ReqSocketReplyTimeOut, instance:
            raise ReqSocketAckTimeOut(str(instance))
            
        assert message.control["accepted"], str(message.control)

