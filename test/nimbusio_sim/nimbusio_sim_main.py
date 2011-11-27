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
from test.nimbusio_sim.db import create_database, start_database

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
    # if args.create, make sure basedir is empty or does not exist
    if config.createnew:
        if os.path.isdir(config.basedir) and os.listdir(config.basedir):
            print >>sys.stderr, "cannot create new cluster %s: not empty" % (
                config.basedir, )
            return False
    # otherwise make sure we have a config file to load
    else:
        if not os.path.exists(config.config_path):
            print >>sys.stderr, "no config found at %s " \
                "(create a new cluster with --create)" % (
                    config.config_path, )
            return False
    return True

def ensure_paths(config):
    "ensure that all the directories needed exist"

    for dirpath in config.required_paths:
        if not os.path.exists(dirpath):
            os.makedirs(dirpath)

def remove_files(topdir):
    "remove files (but not directories) recursively"
    for path, dirs, files in os.walk(topdir):
        for fname in files:
            os.unlink(os.path.join(path, fname))

def main():
    """Main entry point for cluster simulator"""

    args = parse_cmdline()
    config = ClusterConfig(args)
    print repr(args)

    if not sanity_check(config):
        return 1

    if config.createnew:
        ensure_paths(config)
        old_config = config
    else:
        old_config = config
        config = ClusterConfig.load(config)

    if old_config.logprune:
        remove_files(config.log_path)

    if old_config.createnew and not config.systemdb:
        config.database_users.update(create_database(config))
        print "Saving config to %s" % (config.config_path, )
        config.save()
    elif not config.systemdb:
        start_database(config)

    os.environ.update(dict(config.env_for_cluster()))

    #import pdb
    #pdb.set_trace()
    _initialize_logging(config)
    log = logging.getLogger("main")
    log.info("progam starts")

    log.info("entering main loop")
    command_interpreter = CommandInterpreter(config)
    if old_config.start:
        command_interpreter.do_start("all")
    command_interpreter.cmdloop("nimbus.io")
    log.info("leaving main loop")

    log.info("program ends normally")
    return 0

if __name__ == "__main__":
    sys.exit(main())

