# -*- coding: utf-8 -*-
"""
standard_logging.py

common routines for logging
"""
import datetime
import logging
import logging.handlers
import os
import sys

_max_log_size = 16 * 1024 * 1024
_max_log_backup_files = 1000
_log_level_name = os.environ.get("NIMBUSIO_LOG_LEVEL", "INFO")
_log_format_template = '%(asctime)s %(levelname)-8s %(name)-20s: %(message)s'
_log_to_stderr = bool(int(os.environ.get("NIMBUSIO_LOG_TO_STDERR", "0")))


def format_timestamp(timestamp):
    """return python float time.time() as a human readable string"""
    return datetime.datetime.fromtimestamp(timestamp).isoformat()

def initialize_logging(log_path):
    """initialize the log"""
    invalid_log_level = False
    try:
        log_level = logging._levelNames[_log_level_name]
    except KeyError:
        log_level = logging.DEBUG
        invalid_log_level = True

    if _log_to_stderr:
        handler = logging.StreamHandler(stream=sys.stderr)
    else:
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

    if invalid_log_level:
        logging.error("Invalid log level {0}".format(_log_level_name))

