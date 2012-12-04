# -*- coding: utf-8 -*-
"""
redis_connection.py

create a redis connection
"""
import logging
import os

import redis

_redis_host = os.environ.get("REDIS_HOST", "localhost")
_redis_port = int(os.environ.get("REDIS_PORT", str(6379)))
_redis_db = int(os.environ.get("REDIS_DB", str(0)))

def create_redis_connection(host=None, port=None, db=None):
    log = logging.getLogger("create_redis_connection")

    if host is None:
        host = _redis_host
    if port is None:
        port = _redis_port
    if db is None:
        db = _redis_db

    log.info("connecting to {0}:{1} db={2}".format(host, port, db))
    return redis.StrictRedis(host=host, port=port, db=db)

