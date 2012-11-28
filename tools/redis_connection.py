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

def create_redis_connection():
    log = logging.getLogger("create_redis_connection")
    log.info("connecting to {0}:{1} db={2}".format(_redis_host, 
                                                   _redis_port, 
                                                   _redis_db))
    return redis.StrictRedis(host=_redis_host, 
                             port=_redis_port, 
                             db=_redis_db)

