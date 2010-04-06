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


NUM_EXCHANGES = len(os.environ['DIY_NODE_EXCHANGES'].split())
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
        for regex, methods, func_name in self.routes:
            m = regex.match(req.path)
            if not m:
                continue
            if req.method not in methods:
                raise exc.HTTPMethodNotAllowed()
            try:
                method = getattr(self, func_name)
            except AttributeError:
                continue
            return method(req, *m.groups(), **m.groupdict())
        raise exc.HTTPNotFound()

    @routes.add(r'/([^/]+)$', 'POST')
    def archive(self, req, key):
        timestamp = time.time()
        avatar_id = 1001
        archiver = AMQPArchiver(self.amqp_handler)
        encoder = Encoder(MIN_EXCHANGES, NUM_EXCHANGES)
        segments = encoder.encode(req.body)
        archiver.archive_entire(avatar_id, key, segments, timestamp)
        return Response('OK')
