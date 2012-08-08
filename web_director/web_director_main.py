"""
very basic prototype for web director for nimbus.io

all it does so far is try to get traffic to one of the hosts in the appropriate
cluster

does not implement several of the needed capabilities including:
    - health checks for hosts or listen for a heartbeat
    - distinguish between read and write requests and send to different
      destinations
    - pick read hosts based on key consistent hashing for caching
    - pick write hosts based on least busy (busyness info included in
      heartbeats)
    - handle key authentication for reads
    - handle requests for public collections w/o auth
    - warm the cache on startup (select all the clusters, and the mapping of
      collections to clusters in bulk from the database.)
    - expiration of cached values (i.e. when a collections information changes,
      such as becoming public, private, or being migrated to a different
      cluster.)
"""

import gevent

import gevent_psycopg2
gevent_psycopg2.monkey_patch()

import traceback
import logging
import os
import re

#from http_parser.pyparser import HttpParser
from http_parser.parser import HttpParser

from tools.standard_logging import initialize_logging

from web_director.router import Router

ERROR_DELAY = 3.0
MAX_HEADER_LENGTH = 8192

LOG_PATH = "%s/nimbusio_web_director.log" % (os.environ["NIMBUSIO_LOG_DIR"], )

_ROUTER = None
_HOST_PORT_REGEXP = re.compile("^(.*):(\d+)$")

def proxy(data):
    """
    the function called by tproxy to determine where to send traffic

    tproxy will call this function repeatedly for the same connection, as we
    receive more incoming data, until we return something other than None.

    typically our response tells tproxy where to proxy the connection to, but
    may also tell it to hang up, or respond with some error message.
    """

    log = logging.getLogger("proxy")

    bytes_received = len(data)

    parser =  HttpParser()
    bytes_parsed = parser.execute(data, bytes_received)

    if bytes_parsed != bytes_received:
        return { 'close': 
            'HTTP/1.0 400 Bad Request\r\n\r\nParse error' }

    if not parser.is_headers_complete(): 
        if bytes_received > MAX_HEADER_LENGTH:
            return { 'close': 
                'HTTP/1.0 400 Bad Request\r\n'
                '\r\nHeaders are too large' }
        return None

    headers = parser.get_headers()

    # the hostname may be in the form of hostname:port, in which case we want
    # to discard the port, and route just on hostname
    route_host = headers.get('HOST', None)
    if route_host:
        match = _HOST_PORT_REGEXP.match(route_host)
        if match:
            route_host = match.group(1)

    try:
        log.debug("Routing %r" % ( parser.get_url(), ))
        return _ROUTER.route(
            route_host,
            parser.get_method(),
            parser.get_path(),
            parser.get_query_string())
    except Exception, err:
        log.error("error routing %r, %s" % (
            parser.get_url(), traceback.format_exc(), ))
        gevent.sleep(ERROR_DELAY)
        return { 'close': 
            'HTTP/1.0 502 Gateway Error\r\n'
            '\r\nError routing request' }

def init_setup():
    initialize_logging(LOG_PATH)
    log = logging.getLogger("init_setup")
    log.info("setup start")
    global _ROUTER
    _ROUTER = Router()
    gevent.spawn_later(0.0, _ROUTER.init)
    log.info("setup complete")

init_setup()
