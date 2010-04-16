# -*- coding: utf-8 -*-
"""
application.py

The diyapi wsgi application
"""
import re
import os
import time

from webob.dec import wsgify
from webob import exc
from webob import Response

from zfec.easyfec import Encoder
from diyapi_web_server.amqp_archiver import AMQPArchiver
from diyapi_web_server.amqp_listmatcher import AMQPListmatcher


EXCHANGES = os.environ['DIY_NODE_EXCHANGES'].split()
NUM_EXCHANGES = len(EXCHANGES)
MIN_EXCHANGES = NUM_EXCHANGES - 2
SLICE_SIZE = 1024 * 1024    # 1MB


class router(list):
    def add(self, regex, *methods):
        if not methods:
            methods = ('GET', 'HEAD')
        regex = re.compile(regex)
        def dec(func):
            self.append((regex, methods, func.__name__))
            return func
        return dec


class Application(object):
    def __init__(self, amqp_handler):
        self.amqp_handler = amqp_handler

    routes = router()

    @wsgify
    def __call__(self, req):
        url_matched = False
        for regex, methods, func_name in self.routes:
            m = regex.match(req.path)
            if not m:
                continue
            url_matched = True
            if req.method not in methods:
                continue
            try:
                method = getattr(self, func_name)
            except AttributeError:
                continue
            return method(req, *m.groups(), **m.groupdict())
        if url_matched:
            raise exc.HTTPMethodNotAllowed()
        raise exc.HTTPNotFound()

    @routes.add(r'/([^/]+)$', 'POST')
    def archive(self, req, key):
        # TODO: stop hard-coding avatar_id
        avatar_id = 1001
        timestamp = time.time()
        # TODO: split large files into sequences
        archiver = AMQPArchiver(self.amqp_handler, EXCHANGES)
        encoder = Encoder(MIN_EXCHANGES, NUM_EXCHANGES)
        segments = encoder.encode(req.body)
        # TODO: handle archive failure
        archiver.archive_entire(avatar_id, key, segments, timestamp)
        # TODO: send space accounting message
        return Response('OK')

    @routes.add(r'/$')
    def listmatch(self, req):
        avatar_id = 1001
        # TODO: handle request with missing arguments
        prefix = req.GET['prefix']
        matcher = AMQPListmatcher(self.amqp_handler, EXCHANGES)
        # TODO: handle listmatch failure
        # TODO: break up large (>1mb) listmatch response
        keys = matcher.listmatch(avatar_id, prefix)
        return Response(repr(keys))

    @routes.add(r'/([^/]+)$')
    def retrieve(self, req, key):
        pass

    @routes.add(r'/([^/]+)$', 'DELETE')
    @routes.add(r'/([^/]+)/delete$', 'POST')
    def destroy(self, req, key):
        pass
