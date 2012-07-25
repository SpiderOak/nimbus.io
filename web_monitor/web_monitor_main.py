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

 - uses Python "requests" library for the HTTP work.  
   http://docs.python-requests.org/en/latest/index.html

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

from gevent.monkey import patch_all
patch_all(httplib=True)

from gevent.event import Event

from tools.standard_logging import initialize_logging

_log_path = \
    "{0}/nimbusio_web_monitor.log".format(os.environ["NIMBUSIO_LOG_DIR"])

def _handle_sigterm(halt_event):
    halt_event.set()

def _unhandled_greenlet_exception(greenlet_object):
    log = logging.getLogger("_unhandled_greelent_exception")
    try:
        greenlet_object.get()
    except Exception:
        log.exception(str(greenlet_object))

def main():
    """
    main processing module
    """
    return_code = 0
    initialize_logging(_log_path)
    log = logging.getLogger("main")

    halt_event = Event()
    gevent.signal(signal.SIGTERM, _handle_sigterm, halt_event)
    log.info("program starts")

    while not halt_event.is_set():
        halt_event.wait(10)

    log.info("program terminates return code {0}".format(return_code))
    return return_code

if __name__ == "__main__":
    sys.exit(main())


