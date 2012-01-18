# -*- coding: utf-8 -*-
"""
standard_logging.py

common routines for logging
"""
import datetime
import logging
import logging.handlers

_max_log_size = 16 * 1024 * 1024
_max_log_backup_files = 1000

_log_format_template = '%(asctime)s %(levelname)-8s %(name)-20s: %(message)s'

def format_timestamp(timestamp):
    """return python float time.time() as a human readable string"""
    return datetime.datetime.fromtimestamp(timestamp).isoformat()

def initialize_logging(log_path):
    """initialize the log"""
    log_level = logging.DEBUG
    handler = logging.handlers.RotatingFileHandler(
        log_path,
        mode="a", 
        maxBytes=_max_log_size,
        backupCount=_max_log_backup_files,
        encoding="utf-8"
    )
    formatter = logging.Formatter(_log_format_template)
    handler.setFormatter(formatter)

    logging.root.addHandler(handler)
    logging.root.setLevel(log_level)

