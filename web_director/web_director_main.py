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

import uuid
import gevent
import re
from http_parser.http import HttpStream, NoMoreData
from http_parser.reader import SocketReader
import socket

import traceback
import logging
import os

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

def add_x_forwarded_for_header(headers, peername):
    log = logging.getLogger("add_x_forwarded_for_header")
    # I'm not sure what guarantees the parser makes about how the headers are
    # presented to us, so this is overly defensive.

    existing_x_forwarded_for = None
    headers_to_remove = []
    for name in headers:
        if name.strip().lower() == 'x-forwarded-for':
            existing_x_forwarded_for = headers[name]
            headers_to_remove.append(name)
    for name in headers_to_remove: 
        del headers[name]

    if existing_x_forwarded_for:
        new_x_forwarded_for = "%s, %s" % (existing_x_forwarded_for, peername, )
    else:
        new_x_forwarded_for = str(peername)

    headers["X-Forwarded-For"] = new_x_forwarded_for

    # still I don't feel great about this. from a security standpoint,
    # signaling access control information (such as IP address, which some
    # collections set access policy for) inside a stream of data controlled by
    # the attacker is a bad idea.  downstream http parsers are probably robust
    # enough to not be easily trickable, but I'd feel better about signaling
    # out of band, or adding another header with a HMAC from a secret key.

def rewrite_headers(parser, peername, values=None):
    log = logging.getLogger("rewrite_headers")
    headers = parser.headers()
    if isinstance(values, dict):
        headers.update(values)

    add_x_forwarded_for_header(headers, peername)

    httpver = "HTTP/%s" % ".".join(map(str,
                parser.version()))

    new_headers = ["%s %s %s\r\n" % (parser.method(), parser.url(),
        httpver)]

    new_headers.extend(["%s: %s\r\n" % (k, str(v)) for k, v in \
            headers.items()])

    return "".join(new_headers) + "\r\n"

def rewrite_request(req):
    log = logging.getLogger("rewrite_request")
    peername = req._src.getpeername()
    try:
        while True:
            request_id = str(uuid.uuid4())
            parser = HttpStream(req)
            new_headers = rewrite_headers(
                parser, peername, {'x-nimbus-io-user-request-id': request_id})
            if new_headers is None:
                break
            log.debug("rewriting request %s" % ( request_id, ))
            req.send(new_headers)
            body = parser.body_file()
            while True:
                data = body.read(8192)
                if not data:
                    break
                req.writeall(data)
    except (socket.error, NoMoreData):
        pass


def init_setup():
    initialize_logging(LOG_PATH)
    log = logging.getLogger("init_setup")
    log.info("setup start")
    global _ROUTER
    _ROUTER = Router()
    gevent.spawn_later(0.0, _ROUTER.init)
    log.info("setup complete")

init_setup()
