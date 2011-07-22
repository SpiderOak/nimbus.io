# -*- coding: utf-8 -*-
"""
diy_client_main.py

A process that serves as a client to the SpiderOak DIY storage system
Communication is through zeromq
"""
import logging
import os
import os.path
import signal
import sys

import gevent
from gevent import monkey; monkey.patch_socket()
from gevent_zeromq import zmq
from gevent.queue import Queue
from gevent.event import Event

from sample_code.diy_client.pull_server import PULLServer
from sample_code.diy_client.publisher import Publisher
from sample_code.diy_client.message_handler import MessageHandler

_log_path = "diy_client.log"
_log_format_template = u'%(asctime)s %(levelname)-8s %(name)-20s: %(message)s'
_pull_address = "ipc:///tmp/diy-client-main-pull/socket"
_pub_address = "ipc:///tmp/diy-client-main-pub/socket"
_config_path = os.path.expandvars("$HOME/.config/diy-tool/config")

def _handle_sigterm(halt_event):
    halt_event.set()

def _prepare_ipc_path(address):
    """
    IPC sockets need an existing file for an address
    """
    path = address[len("ipc://"):]
    dir_name = os.path.dirname(path)
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)
    if not os.path.exists(path):
        with open(path, "w") as output_file:
            output_file.write("pork")

def _initialize_logging(log_path):
    """initialize the log"""
    handler = logging.FileHandler(log_path, mode="a", encoding="utf-8")
    formatter = logging.Formatter(_log_format_template)
    handler.setFormatter(formatter)

    # define a Handler which writes INFO messages or higher to the sys.stderr
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    formatter = logging.Formatter("%(levelname)-8s %(name)-20s %(message)s")
    console.setFormatter(formatter)
    
    logging.root.addHandler(handler)
    logging.root.addHandler(console)

    logging.root.setLevel(logging.DEBUG)

def _load_config():
    config = dict()
    for line in open(_config_path):
        key, value = line.split()
        config[key] = value
    return config

def main():
    """
    main processing module
    """
    _initialize_logging(_log_path)
    log = logging.getLogger("main")
    log.info("program starts")

    halt_event = Event()
    gevent.signal(signal.SIGTERM, _handle_sigterm, halt_event)

    config = _load_config()
    log.info("running as username '%s'" % (config["Username"], ))

    context = zmq.context.Context()
    send_queue = Queue()
    receive_queue = Queue()

    _prepare_ipc_path(_pull_address)
    _prepare_ipc_path(_pub_address)

    publisher = Publisher(halt_event, context, _pub_address, send_queue)
    message_handler = MessageHandler(
        halt_event, config, send_queue, receive_queue
    )
    pull_server = PULLServer(halt_event, context, _pull_address, receive_queue)
    
    publisher.start()
    message_handler.start()
    pull_server.start()

    log.info("waiting")
    halt_event.wait()
    
    log.info("killing")
    pull_server.kill()
    message_handler.kill()
    publisher.kill()
    
    log.info("joining")
    pull_server.join()
    message_handler.join()
    publisher.join()
    
    context.term()
    log.info("program ends")
    return 0

if __name__ == "__main__":
    sys.exit(main())

