# -*- coding: utf-8 -*-
"""
diy_sim_main.py

Main module for SpiderOak DIY simulator
"""
import logging
import signal
import sys
from threading import Event

from test.diy_sim.node_sim import NodeSim

_log_path = u"/var/log/pandora/diy_sim.log"
_log_format_template = u'%(asctime)s %(levelname)-8s %(name)-20s: %(message)s'
_node_count = 10
_polling_interval = 3.0

def _initialize_logging():
    """initialize the log"""
    log_level = logging.DEBUG
    handler = logging.FileHandler(_log_path, mode="a", encoding="utf-8" )
    formatter = logging.Formatter(_log_format_template)
    handler.setFormatter(formatter)

    logging.root.addHandler(handler)
    logging.root.setLevel(log_level)

def _create_signal_handler(halt_event):
    def cb_handler(*_):
        halt_event.set()
    return cb_handler

def main():
    """Main entry point for cluster simulator"""
    _initialize_logging()
    log = logging.getLogger("main")
    log.info("progam starts")

    halt_event = Event()
    signal.signal(signal.SIGTERM, _create_signal_handler(halt_event))

    test_dir = "/tmp"

    node_sims = [NodeSim(test_dir, i) for i in xrange(_node_count-1)]
    node_sims.append(
        NodeSim(
            test_dir, 
            _node_count-1, 
            space_accounting=True,
            anti_entropy=True
        )
    )

    print "starting node sims with delay"
    for node_sim in node_sims:
        node_sim.start()
        halt_event.wait(1.0)
    
    log.info("entering main loop")
    print "entering main loop"
    while not halt_event.is_set():
        for node_sim in node_sims:
            node_sim.poll()

        try:
            halt_event.wait(_polling_interval)
        except KeyboardInterrupt:
            halt_event.set()
        
    print "leaving main loop"
    log.info("leaving main loop")

    print "stopping node sims"
    for node_sim in node_sims:
        node_sim.stop()

    log.info("program ends normally")
    return 0

if __name__ == "__main__":
    sys.exit(main())

