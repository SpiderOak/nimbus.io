# -*- coding: utf-8 -*-
"""
application.py

The diyapi wsgi application
"""
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


class Application(object):
    def __init__(self, amqp_handler):
        self.amqp_handler = amqp_handler

    @wsgify
    def __call__(self, req):
        timestamp = time.time()
        avatar_id = 1001
        key = req.path.lstrip('/')
        archiver = AMQPArchiver(self.amqp_handler)
        encoder = Encoder(MIN_EXCHANGES, NUM_EXCHANGES)
        segments = encoder.encode(req.body)
        archiver.archive_entire(avatar_id, key, segments, timestamp)
        return Response('OK')
