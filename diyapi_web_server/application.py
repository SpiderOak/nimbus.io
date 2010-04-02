# -*- coding: utf-8 -*-
"""
application.py

The diyapi wsgi application
"""
from webob.dec import wsgify
from webob import exc
from webob import Response

from diyapi_web_server.data_accumulator import DataAccumulator


SLICE_SIZE = 1024 * 1024    # 1MB


class FakeDataDispatcher(object):
    def __init__(self, _):
        self.data = []

    def handle_slice(self, data):
        self.data.append(data)


class Application(object):
    def __init__(self, amqp_handler):
        self.amqp_handler = amqp_handler

    @wsgify
    def __call__(self, req):
        dispatcher = FakeDataDispatcher(self.amqp_handler)
        accumulator = DataAccumulator(req.body_file, SLICE_SIZE, dispatcher)
        accumulator.accumulate()
        return Response('OK')
