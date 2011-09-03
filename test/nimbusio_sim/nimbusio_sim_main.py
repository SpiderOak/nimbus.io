# -*- coding: utf-8 -*-
"""
diy_sim_main.py

Main module for nimbus.io simulator
"""
import logging
import os
import sys

from test.diy_sim.command_interpreter import CommandInterpreter

_log_path = u"%s/nimbusio_sim.log" % (os.environ["NIMBUSIO_LOG_DIR"], )
_log_format_template = u'%(asctime)s %(levelname)-8s %(name)-20s: %(message)s'

def _initialize_logging():
    """initialize the log"""
    log_level = logging.DEBUG
    handler = logging.FileHandler(_log_path, mode="a", encoding="utf-8" )
    formatter = logging.Formatter(_log_format_template)
    handler.setFormatter(formatter)

    logging.root.addHandler(handler)
    logging.root.setLevel(log_level)

def main():
    """Main entry point for cluster simulator"""
    _initialize_logging()
    log = logging.getLogger("main")
    log.info("progam starts")

    log.info("entering main loop")
    command_interpreter = CommandInterpreter()
    command_interpreter.cmdloop("diy")
    log.info("leaving main loop")

    log.info("program ends normally")
    return 0

if __name__ == "__main__":
    sys.exit(main())

