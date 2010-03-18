# -*- coding: utf-8 -*-
"""
diyapi_database_server_main.py

Responds db key lookup requests (mostly from Data Reader)
Responds to db key insert requests (from Data Writer)
Responds to db key list requests from (web components)
Keeps LRU cache of databases open during normal operations.
Databases are simple  key/value stores. 
Every value either points to data or a tombstone and timestamp. 
Every data pointer includes
a timestamp, segment number, size of the segment, 
the combined size of the assembled segments and decoded segments, 
adler32 of the segment, 
and the md5 of the segment.
"""
import errno
import logging
import signal
from socket import error as socket_error
import sys
from threading import Event

import amqplib.client_0_8 as amqp

from tools import amqp_connection

_log_path = u"/var/log/pandora/diyapi_database_server.log"
_log_format_template = u'%(asctime)s %(levelname)-8s %(name)-20s: %(message)s'
_database_server_queue_name = "database_server"

def _initialize_logging():
    """initialize the log"""
    log_level = logging.DEBUG
    handler = logging.FileHandler(_log_path, mode="a", encoding="utf-8" )
    formatter = logging.Formatter(_log_format_template)
    handler.setFormatter(formatter)

    logging.root.addHandler(handler)
    logging.root.setLevel(log_level)

def _create_signal_handler(halt_event, channel, segment_info_tag):
    def cb_handler(signum, frame):
        # Tell the channel we dont want to consume anymore  
        channel.basic_cancel(segment_info_tag)
        halt_event.set()
    return cb_handler

def _create_bindings(channel):
    channel.queue_declare(
        queue=_database_server_queue_name,
        passive=False,
        durable=True,
        exclusive=False,
        auto_delete=False
    )

    channel.queue_bind(
        queue=_database_server_queue_name,
        exchange=amqp_connection.local_exchange_name,
        routing_key="database_server.*" 
    )

_dispatch_table = {
}

def _process_message(connection, message):
    log = logging.getLogger("_process_message")

    routing_key = message.delivery_info["routing_key"]
    if not routing_key in _dispatch_table:
        log.error("unknown routing key '%s'" % (routing_key, ))
        return

    _dispatch_table[routing_key](connection, message.body)

def _process_message_wrapper(connection, message):
    log = logging.getLogger("_process_message_wrapper")
    try:
        _process_message(connection, message)
    except Exception, instance:
        log.exception(instance)

def _callback_closure(connection):
    def __callback(message):
        _process_message_wrapper(connection, message)
    return __callback

def _run_until_halt():
    log = logging.getLogger("_run_until_halt")

    halt_event = Event()

    connection = amqp_connection.open_connection()
    channel = connection.channel()
    amqp_connection.create_exchange(channel)
    _create_bindings(channel)

    # Let AMQP know to send us messages
    amqp_tag = channel.basic_consume( 
        queue=_database_server_queue_name, 
        no_ack=True,
        callback=_callback_closure(connection)
    )

    signal.signal(
        signal.SIGTERM, 
        _create_signal_handler(halt_event, channel, amqp_tag)
    )

    log.debug("start AMQP loop")
    # 2010-03-18 dougfort -- channel wait does a blocking read, 
    # it gets [Errno 4] Interrupted system call on SIGTERM 
    while not halt_event.is_set():
        try:
            channel.wait()
        except socket_error, instance:
            if instance.errno == errno.EINTR:
                log.warn("Interrupted system call: assuming SIGTERM %s" % (
                    instance
                ))
                halt_event.set()
            else:
                raise
    log.debug("end AMQP loop")

    channel.close()
    connection.close()

def main():
    """main processing entry point"""
    _initialize_logging()
    log = logging.getLogger("main")
    log.info("start")

    try:
        _run_until_halt()
    except Exception, instance:
        log.exception(instance)
        print >> sys.stderr, instance.__class__.__name__, str(instance)
        return 12

    log.info("normal termination")
    return 0

if __name__ == "__main__":
    sys.exit(main())

