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

from zfec.easyfec import Encoder, Decoder
from diyapi_web_server.amqp_archiver import AMQPArchiver
from diyapi_web_server.amqp_listmatcher import AMQPListmatcher
from diyapi_web_server.amqp_retriever import AMQPRetriever


SLICE_SIZE = 1024 * 1024    # 1MB


class router(list):
    def add(self, regex, *methods, **query_args):
        if not methods:
            methods = ('GET', 'HEAD')
        regex = re.compile(regex)
        for k in query_args:
            query_args[k] = re.compile(query_args[k])
        def dec(func):
            self.append((regex, query_args, methods, func.__name__))
            return func
        return dec


class Application(object):
    def __init__(self, amqp_handler, exchange_manager):
        self.amqp_handler = amqp_handler
        self.exchange_manager = exchange_manager

    routes = router()

    @wsgify
    def __call__(self, req):
        url_matched = False
        for regex, query_args, methods, func_name in self.routes:
            url_match = regex.match(req.path)
            if not url_match:
                continue
            args_matched = False
            for arg, arg_regex in query_args.iteritems():
                if arg not in req.GET:
                    break
                arg_match = arg_regex.match(req.GET[arg])
                if not arg_match:
                    break
            else:
                args_matched = True
            if not args_matched:
                continue
            url_matched = True
            if req.method not in methods:
                continue
            try:
                method = getattr(self, func_name)
            except AttributeError:
                continue
            return method(req, *url_match.groups(), **url_match.groupdict())
        if url_matched:
            raise exc.HTTPMethodNotAllowed()
        raise exc.HTTPNotFound()

    @routes.add(r'/data/(.+)$', action='listmatch')
    def listmatch(self, req, prefix):
        delimiter = req.GET.get('delimiter', '/')
        avatar_id = 1001
        matcher = AMQPListmatcher(self.amqp_handler, self.exchange_manager)
        # TODO: handle listmatch failure
        # TODO: break up large (>1mb) listmatch response
        keys = matcher.listmatch(avatar_id, prefix)
        return Response(repr(keys))

    @routes.add(r'/data/(.+)$', 'DELETE')
    @routes.add(r'/data/(.+)$', 'POST', action='delete')
    def destroy(self, req, key):
        pass

    @routes.add(r'/data/(.+)$')
    def retrieve(self, req, key):
        avatar_id = 1001
        retriever = AMQPRetriever(self.amqp_handler, self.exchange_manager)
        segments = retriever.retrieve(avatar_id, key)
        while len(segments) > self.exchange_manager.min_exchanges:
            segments.popitem()
        decoder = Decoder(self.exchange_manager.min_exchanges,
                          self.exchange_manager.num_exchanges)
        data = decoder.decode(segments.values(),
                              segments.keys(),
                              0)
        return Response(data)

    @routes.add(r'/data/(.+)$', 'POST')
    def archive(self, req, key):
        # TODO: stop hard-coding avatar_id
        avatar_id = 1001
        timestamp = time.time()
        # TODO: split large files into sequences
        archiver = AMQPArchiver(self.amqp_handler, self.exchange_manager)
        encoder = Encoder(self.exchange_manager.min_exchanges,
                          self.exchange_manager.num_exchanges)
        segments = encoder.encode(req.body)
        # TODO: handle archive failure
        archiver.archive_entire(avatar_id, key, segments, timestamp)
        # TODO: send space accounting message
        return Response('OK')
