# -*- coding: utf-8 -*-
"""
memcached_client.py

common code for creating a memcached client
"""
import os

import memcache

_memcached_host = os.environ.get("NIMBUSIO_MEMCACHED_HOST", "localhost")
_memcached_port = int(os.environ.get("NIMBUSIO_MEMCACHED_PORT", "11211"))
_memcached_nodes = ["{0}:{1}".format(_memcached_host, _memcached_port), ]

def create_memcached_client():
    return memcache.Client(_memcached_nodes)
