# -*- coding: utf-8 -*-
"""
web_monitor_main.py

 - Web monitor uses a configuration file to know which web services to check.
 - It uses a single gevent process instead of ZMQ, 
   spawning one greenlet per service to monitor.  
 - It updates Redis with the status of each check.
 - It does not run in the context of a storage node, but rather in the context 
   of an infrastructure node 
   (i.e. it runs from /etc/service/nimbus.io.web_monitor, 
   not /etc.01/service/nimbus.io.web_monitor.)  
   This is the same as the current web_director.  
   (/etc/service/nimbus.io.web_director)

 - implements the timeouts via gevent.Timeout.  
   http://www.gevent.org/gevent.html#timeouts

 - Must call gevent monkey patch like this, 
   because it does not patch httplib by default: 
   gevent.monkey.patch_all(httplib=True)

 - Stores reachability results in a redis hash structure.  
   The name of the hash is the "nimbus.io.web_monitor.$HOSTNAME".  
   (i.e. one hash per monitor.)  
   The keys of the hash are the HOST:PORT of each service we are monitoring.  
   The values are the result of the check. 
   (Can be a JSON string if more detail is needed.)

   http://redis.io/topics/data-types#hashes  

 - Rather than try to mess with a Redis connection pool, just spawns a single 
   greenlet to talk to Redis.  That greenlet connects to Redis, then blocks on 
   popping from a queue of updates.  Every other greenlet just pushes check 
   result updates onto that queue.  No fancy connection pool needed.  

"""
import json
import logging
import os
import os.path
import signal
import sys

import gevent
import gevent.queue

from gevent.monkey import patch_all
patch_all(httplib=True)

from gevent.event import Event

from tools.standard_logging import initialize_logging

from web_monitor.web_monitor_redis_sink import WebMonitorRedisSink
from web_monitor.pinger import Pinger

_log_path = \
    "{0}/nimbusio_web_monitor.log".format(os.environ["NIMBUSIO_LOG_DIR"])
_polling_interval = 3.0
_return_code = 0

def _handle_sigterm(halt_event):
    halt_event.set()

def _unhandled_greenlet_exception(greenlet_object):
    log = logging.getLogger("_unhandled_greelent_exception")
    try:
        greenlet_object.get()
    except Exception:
        log.exception(str(greenlet_object))

def _redis_exception_closure(halt_event):
    global _return_code

    def __redis_sink_exception_handler(greenlet_object):
        global _return_code

        log = logging.getLogger("__redis_sink_exception_handler")
        try:
            greenlet_object.get()
        except Exception:
            log.exception(str(greenlet_object))
        _return_code = 1
        halt_event.set()

    return __redis_sink_exception_handler

def main():
    """
    main processing module
    """
    global _return_code

    initialize_logging(_log_path)
    log = logging.getLogger("main")

    halt_event = Event()
    gevent.signal(signal.SIGTERM, _handle_sigterm, halt_event)
    log.info("program starts")

    redis_queue = gevent.queue.Queue()
    greenlets = list()

    try:
        config_path = sys.argv[1]
        log.info("reading config from '{0}'".format(config_path))
        with open(config_path) as input_file:
            config = json.load(input_file)

        redis_sink = WebMonitorRedisSink(halt_event, redis_queue)
        redis_sink.link_exception(_redis_exception_closure(halt_event))
        redis_sink.start()

        greenlets.append(redis_sink)

        for config_entry in config:
            pinger = Pinger(halt_event, 
                            _polling_interval, 
                            redis_queue, 
                            config_entry)

            pinger.link_exception(_unhandled_greenlet_exception)
            pinger.start()

            greenlets.append(pinger)

    except Exception as instance:
        log.exception(instance)
        _return_code = 1

    # wait here while the pingers do their job
    halt_event.wait()

    for entry in greenlets:
        entry.join(timeout=3.0)

    log.info("program terminates return code {0}".format(_return_code))
    return _return_code

if __name__ == "__main__":
    sys.exit(main())


