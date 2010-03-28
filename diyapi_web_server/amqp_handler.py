import errno
import logging
from socket import error as socket_error

import gevent
from gevent.event import Event

from tools import amqp_connection
from tools.message_driven_process import _create_bindings, _callback_closure


_queue_name = "web_server"
_routing_key_binding = "web_server.*"


class AMQPHandler(object):
    def __init__(self):
        self.halt_event = Event()
        self._stopped_event = Event()
        self.log = logging.getLogger('AMQPHandler')

        self.state = {}
        self.queue_name = _queue_name
        self.routing_key_binding = _routing_key_binding

        self.connection = amqp_connection.open_connection()
        self.channel = self.connection.channel()
        amqp_connection.create_exchange(self.channel)
        _create_bindings(self.channel, self.queue_name, self.routing_key_binding)

        # Let AMQP know to send us messages
        self.amqp_tag = self.channel.basic_consume(
            queue=self.queue_name,
            no_ack=True,
            callback=_callback_closure(self.state, self.connection, self.dispatch_table)
        )

    def _run(self):
        self.log.debug("start AMQP loop")
        while not self.halt_event.is_set():
            try:
                self.channel.wait()
            except (KeyboardInterrupt, SystemExit):
                self.log.info("KeyboardInterrupt or SystemExit")
                self.halt_event.set()
            except socket_error, instance:
                if instance.errno == errno.EINTR:
                    self.log.warn("Interrupted system call: assuming SIGTERM %s" % (
                        instance
                    ))
                    self.halt_event.set()
                else:
                    raise
        self.log.debug("end AMQP loop")

        self.channel.close()
        self.connection.close()

        self._stopped_event.set()

    def start(self):
        gevent.spawn(self._run)

    def _stop(self):
        self.channel.basic_cancel(self.amqp_tag)
        self.halt_event.set()

    def stop(self):
        gevent.spawn(self._stop)
        self._stopped_event.wait()

    dispatch_table = {}
