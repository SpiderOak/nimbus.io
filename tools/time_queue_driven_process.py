# -*- coding: utf-8 -*-
"""
nimbus.io back-end servers perform two kinds of tasks:
 * message-driven
 * periodic
 
The message-driven tasks are periodic also, in the sense that they get messages
by periodically polling a zeromq dealer (xreq) socket.

All the back-end servers run a series of small tasks, 
 * processing an incoming message and sending a reply
 * performing some recurring job

   * polling zeromq socket(s) for incoming messages
   * garbage collection
   * statistics reporting
   * etc

The tasks are organized in a :ref:`time-queue-label`

The server's processing loop is:
 * wait for the next task's time to come due
 * pop the task from the time queue
 * execute the task
 * add new tasks to the time queue

Each task can return a list of tuples to define new tasks to be added to
The time queue. A task can 'loop' by returning its own callback function.

This behavior is the same on all back-end servers, so we have encapsulated
the code in a 'time queue driven process'.

The server is driven by callback functions passed in at startup::

    if __name__ == "__main__":
        state = _create_state()
        sys.exit(
            time_queue_driven_process.main(
                _log_path,
                state,
                pre_loop_actions=[_setup, ],
                post_loop_actions=[_tear_down, ],
                exception_action=exception_event
            )
        )

"""
import logging
import sys
from threading import Event
import time

from tools.time_queue import TimeQueue
from tools.standard_logging import initialize_logging
from tools.process_util import set_signal_handler

def _run_until_halt(
    state,
    pre_loop_actions,
    post_loop_actions,
    halt_event
):
    log = logging.getLogger("_run_until_halt")

    set_signal_handler(halt_event)

    time_queue = TimeQueue()

    # run any pre-loop actions.
    # they have the opportunity to modify state
    # and can return tasks to be added to the time queue
    # if a pre-loop actions raises an exception, we must halt
    log.debug("pre_loop_action(s)")
    for pre_loop_action in pre_loop_actions:
        if halt_event.is_set():
            log.info("halt_event signaled during pre_loop")
            return
        try:
            result_list = pre_loop_action(halt_event, state)
        except Exception:
            halt_event.set()
            raise
        if result_list is not None:
            for task, start_time in result_list:
                time_queue.put(task, start_time=start_time)

    log.info("main loop starts")
    
    # run until the time queue is empty. This means each task
    # must watch the halt_event and behave correctly at shutdown.
    while len(time_queue) > 0:

        next_task_delay = time_queue.peek_time() - time.time()
        if next_task_delay > 0.0:
            halt_event.wait(next_task_delay)

        next_task = time_queue.pop()
        result_list = next_task(halt_event)
        if result_list is not None:
            for task, start_time in result_list:
                time_queue.put(task, start_time=start_time)
            
    log.info("main loop ends")

    log.debug("post_loop_action(s)")
    for post_loop_action in post_loop_actions:
        post_loop_action(state)
    log.info("program terminates")

def main(
    log_path, 
    state,
    pre_loop_actions,
    post_loop_actions,
    exception_action = None,
    halt_event = Event(),
):
    """
    This function is run as the main entry point of every time queue driven
    process. It wraps the event loop driven by te time_queue.
    
    log_path
        The full path to the log file for this process

    state
        State object (usually a dict) passed to callback functions

    pre_loop_actions
        A list of functions to be run before the event loop starts. 

        Function arguments are ``(halt_event, state)``

        Functions may return a list of tuples to be added to the time queue
        ``(callback_function, start_time, )``

        nimbus.io processes this function is conventionlly run a single
        function called ``_startup``, but it can have any name.

    post_loop_actions
        A list of functions to be run after the event loop terminates 
        (``halt_event`` set)

        Function argument is  ``(state)``

        Function returns is ignored

        In nimbus.io processes this function is conventioanlly named
        ``_tear_down``, but it can have any name.

    exception_action
        A frunction to be executed when the event loop catches and
        exception fromma calback function.
        
        It takes ``(state)`` as an argument

        Its return is ignored

        In nimbus.io processes this function is used to push a zeromq
        message to an event publisher

    halt_event (optional)
        a ``threading.Event`` that will be set when SIGTERM is detected
        used to terminate the event loop.

        halt_event is passed to as an argument to callback functions.
        
        If the caller wants greater access, they can create their
        own halt event and pass it here as an argument.

    returns 0 for normal termination, nonzero for failure
        
    """
    initialize_logging(log_path)
    log = logging.getLogger("main")
    log.info("start")

    while not halt_event.is_set():
        try:
            _run_until_halt(
                state,
                pre_loop_actions,
                post_loop_actions,
                halt_event
            )
        except Exception, instance:
            log.exception(instance)
            print >> sys.stderr, instance.__class__.__name__, str(instance)
            if exception_action is not None:
                exception_action(state)
            return 12

    log.info("normal termination")
    return 0

