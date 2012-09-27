# -*- coding: utf-8 -*-
"""
req_socket.py

wrap a zeromq REQ socket, with check for ack
"""
import logging
import time

from gevent_zeromq import zmq

from tools.zeromq_util import prepare_ipc_path

class ReqSocketError(Exception):
    pass
class ReqSocketAckTimeOut(ReqSocketError):
    pass

_timeout_seconds = 15.0

class ReqSocket(object):
    """
    wrap a zeromq REQ socket, with check for ack
    """
    def __init__(self, zmq_context, address, halt_event):
        self._name = address
        self._log = logging.getLogger(self._name)

        self._socket = zmq_context.socket(zmq.REQ)

        # we need a valid path for IPC sockets
        if address.startswith("ipc://"):
            prepare_ipc_path(address)

        self._log.info("connecting to {0}".format(address))
        self._socket.connect(address)

        self._halt_event = halt_event

    def __str__(self):
        return self._name

    def close(self):
        self._socket.close()

    def send(self, message, data=None):
        if data is None:
            self._socket.send_json(message)
        else:
            self._socket.send_json(message, zmq.SNDMORE)
            for data_segment in data[:-1]:
                self._socket.send(data_segment, zmq.SNDMORE)
            self._socket.send(data_segment[-1])

    def wait_for_ack(self):
        """
        return when ack is received
        raise ReqSocketAckTimeout if ack is not received
        """
        # 2012-09-06 dougfort -- gevent.Timeout goes off into outer space here
        start_time = time.time()
        while not self._halt_event.is_set():
            try:
                reply = self._socket.recv_json(zmq.NOBLOCK)
            except zmq.ZMQError, instance:
                if instance.errno == zmq.EAGAIN:
                    elapsed_time = time.time() - start_time
                    if elapsed_time < _timeout_seconds:
                        self._halt_event.wait(1.0)
                        continue
                    self.close()
                    error_message = "Timout waiting ack {0} seconds".format(
                        _timeout_seconds)
                    self._log.error(error_message)
                    raise ReqSocketAckTimeOut(error_message)
                raise
            else:
                assert reply["accepted"] 
                return

