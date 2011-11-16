# -*- coding: utf-8 -*-
"""
nimbusio_sim_main.py

Main module for nimbus.io simulator
"""
import logging
import os
import sys

from test.nimbusio_sim.command_interpreter import CommandInterpreter
from test.nimbusio_sim.options import parse_cmdline
from test.nimbusio_sim.cluster_config import ClusterConfig

_log_name = u"nimbusio_sim.log"
_log_format_template = u'%(asctime)s %(levelname)-8s %(name)-20s: %(message)s'

def _initialize_logging(config):
    """initialize the log"""
    log_level = logging.DEBUG
    handler = logging.FileHandler(
        os.path.join(config.log_path, _log_name), mode="a", encoding="utf-8" )
    formatter = logging.Formatter(_log_format_template)
    handler.setFormatter(formatter)

    logging.root.addHandler(handler)
    logging.root.setLevel(log_level)

def sanity_check(config):
    # TODO
    # if args.create, make sure basedir is empty or does not exist
    # if not args.create, make sure basedir has a config file in it
    # make sure all ports in range are bindable
    return True

def ensure_paths(config):
    "ensure that all the directories needed exist"
    # TODO
    pass

def main():
    """Main entry point for cluster simulator"""

    args = parse_cmdline()
    config = ClusterConfig(args)
    print repr(args)

    if not sanity_check(config):
        return 1

    ensure_paths(config)

    #import pdb
    #pdb.set_trace()
    _initialize_logging(config)
    return
    log = logging.getLogger("main")
    log.info("progam starts")

    log.info("entering main loop")
    command_interpreter = CommandInterpreter()
    command_interpreter.cmdloop("nimbus.io")
    log.info("leaving main loop")

    log.info("program ends normally")
    return 0

if __name__ == "__main__":
    sys.exit(main())

