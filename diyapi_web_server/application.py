# -*- coding: utf-8 -*-
"""
application.py

The diyapi wsgi application
"""
import re
import os
import time
import zlib
import hashlib

from webob.dec import wsgify
from webob import exc
from webob import Response

from zfec.easyfec import Encoder, Decoder
from diyapi_web_server import util
from diyapi_web_server.amqp_archiver import AMQPArchiver
from diyapi_web_server.amqp_listmatcher import AMQPListmatcher
from diyapi_web_server.amqp_retriever import AMQPRetriever
from diyapi_web_server.amqp_destroyer import AMQPDestroyer


EXCHANGE_TIMEOUT = 5        # sec
SLICE_SIZE = 1024 * 1024    # 1MB


class router(list):
    # TODO: document and test this
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
    def __init__(self, amqp_handler, exchange_manager, authenticator):
        self.amqp_handler = amqp_handler
        self.exchange_manager = exchange_manager
        self.authenticator = authenticator

    routes = router()

    @wsgify
    def __call__(self, req):
        # TODO: test this
        req.diy_username = util.get_username_from_req(req) or 'test'
        if not self.authenticator.authenticate(req):
            raise exc.HTTPUnauthorized()
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
        # TODO: do something with delimiter
        avatar_id = req.remote_user
        matcher = AMQPListmatcher(self.amqp_handler, self.exchange_manager)
        # TODO: handle listmatch failure
        # TODO: break up large (>1mb) listmatch response
        keys = matcher.listmatch(avatar_id, prefix)
        return Response(repr(keys))

    @routes.add(r'/data/(.+)$', 'DELETE')
    @routes.add(r'/data/(.+)$', 'POST', action='delete')
    def destroy(self, req, key):
        avatar_id = req.remote_user
        timestamp = time.time()
        destroyer = AMQPDestroyer(self.amqp_handler, self.exchange_manager)
        size_deleted = destroyer.destroy(avatar_id, key, timestamp)
        # TODO: send space accounting message
        return Response('OK')

    @routes.add(r'/data/(.+)$')
    def retrieve(self, req, key):
        avatar_id = req.remote_user
        retriever = AMQPRetriever(self.amqp_handler, self.exchange_manager)
        segments = retriever.retrieve(avatar_id, key, 8) # TODO: min_segments
        # TODO: handle retrieve failure
        # TODO: handle multiple slices
        # TODO: check data integrity
        decoder = Decoder(8, # TODO: min_segments
                          self.exchange_manager.num_exchanges)
        segment_nums = map(lambda i: i - 1, segments.keys())
        segment_data = segments.values()
        data = decoder.decode(segment_data, segment_nums, 0)
        return Response(data)

    @routes.add(r'/data/(.+)$', 'POST')
    def archive(self, req, key):
        avatar_id = req.remote_user
        timestamp = time.time()
        # TODO: split large files into slices
        encoder = Encoder(
            8, # TODO: min_segments
            self.exchange_manager.num_exchanges)
        segments = encoder.encode(req.body)
        file_adler32 = zlib.adler32(req.body)
        file_md5 = hashlib.md5(req.body).digest()
        archiver = AMQPArchiver(self.amqp_handler, self.exchange_manager)
        # TODO: handle archive failure
        previous_size = archiver.archive_entire(
            avatar_id,
            key,
            file_adler32,
            file_md5,
            segments,
            timestamp,
            EXCHANGE_TIMEOUT
        )
        # TODO: send space accounting message
        return Response('OK')
