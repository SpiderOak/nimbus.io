# -*- coding: utf-8 -*-
"""
standard_logging.py

common routines for logging
"""
import datetime
import logging

_log_format_template = u'%(asctime)s %(levelname)-8s %(name)-20s: %(message)s'

def format_timestamp(timestamp):
    """return python float time.time() as a human readable string"""
    return datetime.datetime.fromtimestamp(timestamp).isoformat()

def initialize_logging(log_path):
    """initialize the log"""
    log_level = logging.DEBUG
    handler = logging.FileHandler(log_path, mode="a", encoding="utf-8" )
    formatter = logging.Formatter(_log_format_template)
    handler.setFormatter(formatter)

    logging.root.addHandler(handler)
    logging.root.setLevel(log_level)

