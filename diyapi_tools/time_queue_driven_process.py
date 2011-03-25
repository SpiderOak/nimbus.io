# -*- coding: utf-8 -*-
"""
time_queue_driven_process.py

A framework for process that are driven by TimeQueue
"""
import logging
import signal
import sys
from threading import Event
import time

from diyapi_tools.time_queue import TimeQueue
from diyapi_tools.standard_logging import initialize_logging

def _create_signal_handler(halt_event):
    def cb_handler(*_):
        halt_event.set()
    return cb_handler

def _run_until_halt(
    state,
    pre_loop_actions,
    post_loop_actions,
    halt_event
):
    log = logging.getLogger("_run_until_halt")

    signal.signal(signal.SIGTERM, _create_signal_handler(halt_event))

    time_queue = TimeQueue()

    # run any pre-loop actions.
    # they have the opportunity to modify state
    # and can return tasks to be added to the time queue
    log.debug("pre_loop_action(s)")
    for pre_loop_action in pre_loop_actions:
        if halt_event.is_set():
            log.info("halt_event signaled during pre_loop")
            return
        result_list = pre_loop_action(halt_event, state)
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
    halt_event = Event(),
):
    """main processing entry point"""
    initialize_logging(log_path)
    log = logging.getLogger("main")
    log.info("start")

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
        return 12

    log.info("normal termination")
    return 0

