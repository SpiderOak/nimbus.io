# -*- coding: utf-8 -*-
"""
application.py

The diyapi wsgi application
"""
import time

from webob.dec import wsgify
from webob import exc
from webob import Response

from diyapi_web_server.amqp_data_archiver import AMQPDataArchiver


SLICE_SIZE = 1024 * 1024    # 1MB


class Application(object):
    def __init__(self, amqp_handler):
        self.amqp_handler = amqp_handler

    @wsgify
    def __call__(self, req):
        timestamp = time.time()
        avatar_id = 1001
        key = req.path.lstrip('/')
        archiver = AMQPDataArchiver(self.amqp_handler)
        archiver.archive_entire(avatar_id, key, req.body, timestamp)
        return Response('OK')
