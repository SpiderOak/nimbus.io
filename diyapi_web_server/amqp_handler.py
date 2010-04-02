import errno
import logging
from socket import error as socket_error

import gevent

from tools import amqp_connection
from tools.message_driven_process import _create_bindings, _callback_closure


_queue_name = "web_server"
_routing_key_binding = "web_server.*"


class AMQPHandler(object):
    def __init__(self):
        self._chan_wait = None
        self.log = logging.getLogger('AMQPHandler')

        self.state = {}
        self.queue_name = _queue_name
        self.routing_key_binding = _routing_key_binding

    def _run(self):
        self.log.debug("start AMQP loop")
        try:
            while True:
                try:
                    self.channel.wait()
                except (KeyboardInterrupt, SystemExit):
                    self.log.info("KeyboardInterrupt or SystemExit")
                    return
                except socket_error, instance:
                    if instance.errno == errno.EINTR:
                        self.log.warn("Interrupted system call: assuming SIGTERM %s" % (
                            instance
                        ))
                        return
                    else:
                        raise
        except gevent.GreenletExit:
            self.log.info("GreenletExit")
        self.log.debug("end AMQP loop")

        self.channel.basic_cancel(self.amqp_tag)
        self.channel.close()
        self.connection.close()

    def start(self):
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

        self._chan_wait = gevent.spawn(self._run)

    def stop(self):
        if self._chan_wait:
            chan_wait = self._chan_wait
            self._chan_wait = None
            chan_wait.kill(block=True)

    dispatch_table = {}
