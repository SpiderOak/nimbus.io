# -*- coding: utf-8 -*-
"""
message_driven_process.py

A framework for process that are driven by AMQP messages
"""
from collections import deque
import errno
import logging
import signal
from socket import error as socket_error
import sys
from threading import Event

import amqplib.client_0_8 as amqp

from tools import amqp_connection
from tools.standard_logging import initialize_logging

def _create_signal_handler(halt_event, channel, amqp_tag):
    def cb_handler(signum, frame):
        # Tell the channel we dont want to consume anymore  
        channel.basic_cancel(amqp_tag)
        halt_event.set()
    return cb_handler

def _create_bindings(channel, queue_name, routing_key_binding):
    channel.queue_declare(
        queue=queue_name,
        passive=False,
        durable=True,
        exclusive=False,
        auto_delete=False
    )

    channel.queue_bind(
        queue=queue_name,
        exchange=amqp_connection.local_exchange_name,
        routing_key=routing_key_binding 
    )

def _process_outgoing_traffic(channel, outgoing_queue):
    log = logging.getLogger("_process_outgoing_traffic")
    while True:
        try:
            exchange, routing_key, message = outgoing_queue.popleft()
        except IndexError:
            break

        log.debug("exchange = '%s', routing_key = '%s'" % (
            exchange, routing_key
        ))

        amqp_message = amqp.Message(message.marshall())
        channel.basic_publish( 
            amqp_message, 
            exchange=exchange, 
            routing_key=routing_key,
            mandatory = True
        )

def _process_message(state, outgoing_queue, dispatch_table, message):
    """
    process an incoming message, based on routing key
    we call a function in the dispatch table, giving it the state dict
    and the raw incoming string.
    The function must unmarshall the string into a message,
    process the message,
    and return a list of zero or more reply messages.    
    """
    log = logging.getLogger("_process_message")

    routing_key = message.delivery_info["routing_key"]
    if not routing_key in dispatch_table:
        log.error("unknown routing key '%s'" % (routing_key, ))
        return

    outgoing = dispatch_table[routing_key](state, message.body)
    if outgoing is not None:
        outgoing_queue.extend(outgoing)

def _process_message_wrapper(state, outgoing_queue, dispatch_table, message):
    log = logging.getLogger("_process_message_wrapper")
    try:
        _process_message(state, outgoing_queue, dispatch_table, message)
    except Exception, instance:
        log.exception(instance)

def _callback_closure(state, outgoing_queue, dispatch_table):
    def __callback(message):
        _process_message_wrapper(
            state, outgoing_queue, dispatch_table, message
        )
    return __callback

def _run_until_halt(
    queue_name, 
    routing_key_bindings, 
    dispatch_table, 
    state,
    pre_loop_function,
    in_loop_function,
    post_loop_function
):
    log = logging.getLogger("_run_until_halt")

    halt_event = Event()

    connection = amqp_connection.open_connection()
    channel = connection.channel()
    amqp_connection.create_exchange(channel)
    _create_bindings(channel, queue_name, routing_key_bindings)

    outgoing_queue = deque()

    # Let AMQP know to send us messages
    amqp_tag = channel.basic_consume( 
        queue=queue_name, 
        no_ack=True,
        callback=_callback_closure(state, outgoing_queue, dispatch_table)
    )

    signal.signal(
        signal.SIGTERM, 
        _create_signal_handler(halt_event, channel, amqp_tag)
    )

    if pre_loop_function is not None:
        log.debug("pre_loop_function")
        outgoing_queue.extend(pre_loop_function(halt_event, state))

    log.debug("start AMQP loop")
    # 2010-03-18 dougfort -- channel wait does a blocking read, 
    # it gets [Errno 4] Interrupted system call on SIGTERM 
    while not halt_event.is_set():
        
        _process_outgoing_traffic(channel, outgoing_queue)

        try:
            channel.wait()
        except (KeyboardInterrupt, SystemExit):
            log.info("KeyboardInterrupt or SystemExit")
            halt_event.set()
        except socket_error, instance:
            if instance.errno == errno.EINTR:
                log.warn("Interrupted system call: assuming SIGTERM %s" % (
                    instance
                ))
                halt_event.set()
            else:
                raise

        if in_loop_function is not None:
            outgoing_queue.extend(in_loop_function(state))

    log.debug("end AMQP loop")

    if post_loop_function is not None:
        log.debug("post_loop_function")
        outgoing_queue.extend(post_loop_function(state))
        _process_outgoing_traffic(channel, outgoing_queue)

    channel.close()
    connection.close()

def main(
    log_path, 
    queue_name, 
    routing_key_binding, 
    dispatch_table, 
    state,
    pre_loop_function=None,
    in_loop_function=None,
    post_loop_function=None
):
    """main processing entry point"""
    initialize_logging(log_path)
    log = logging.getLogger("main")
    log.info("start")

    try:
        _run_until_halt(
            queue_name, 
            routing_key_binding, 
            dispatch_table, 
            state,
            pre_loop_function,
            in_loop_function,
            post_loop_function
        )
    except Exception, instance:
        log.exception(instance)
        print >> sys.stderr, instance.__class__.__name__, str(instance)
        return 12

    log.info("normal termination")
    return 0

if __name__ == "__main__":
    sys.exit(main())

