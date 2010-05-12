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

from diyapi_web_server import util
from diyapi_web_server.zfec_segmenter import ZfecSegmenter
from diyapi_web_server.amqp_archiver import AMQPArchiver
from diyapi_web_server.amqp_listmatcher import AMQPListmatcher
from diyapi_web_server.amqp_retriever import AMQPRetriever
from diyapi_web_server.amqp_destroyer import AMQPDestroyer
from diyapi_web_server.data_slicer import DataSlicer


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
        keys = matcher.listmatch(avatar_id, prefix, EXCHANGE_TIMEOUT)
        return Response(repr(keys))

    @routes.add(r'/data/(.+)$', 'DELETE')
    @routes.add(r'/data/(.+)$', 'POST', action='delete')
    def destroy(self, req, key):
        avatar_id = req.remote_user
        timestamp = time.time()
        destroyer = AMQPDestroyer(self.amqp_handler, self.exchange_manager)
        size_deleted = destroyer.destroy(
            avatar_id, key, timestamp, EXCHANGE_TIMEOUT)
        # TODO: send space accounting message
        return Response('OK')

    @routes.add(r'/data/(.+)$')
    def retrieve(self, req, key):
        avatar_id = req.remote_user
        retriever = AMQPRetriever(
            self.amqp_handler,
            self.exchange_manager,
            avatar_id,
            key,
            self.exchange_manager.num_exchanges,
            8 # TODO: min_segments
        )
        def response_iter():
            for segments in retriever.retrieve(EXCHANGE_TIMEOUT):
                # TODO: handle retrieve failure
                # TODO: check data integrity
                segmenter = ZfecSegmenter(
                    8, # TODO: min_segments
                    self.exchange_manager.num_exchanges)
                yield segmenter.decode(segments.values())
        return Response(app_iter=response_iter())

    @routes.add(r'/data/(.+)$', 'POST')
    def archive(self, req, key):
        avatar_id = req.remote_user
        timestamp = time.time()
        archiver = AMQPArchiver(
            self.amqp_handler,
            self.exchange_manager,
            avatar_id,
            key,
            timestamp
        )
        segmenter = ZfecSegmenter(
            8, # TODO: min_segments
            self.exchange_manager.num_exchanges)
        file_adler32 = zlib.adler32('')
        file_md5 = hashlib.md5()
        remaining = req.content_length
        for slice in DataSlicer(req.body_file, SLICE_SIZE, remaining):
            remaining -= len(slice)
            file_adler32 = zlib.adler32(slice, file_adler32)
            file_md5.update(slice)
            segments = segmenter.encode(slice)
            # TODO: handle archive failure
            if remaining:
                archiver.archive_slice(
                    segments,
                    EXCHANGE_TIMEOUT
                )
            else:
                previous_size = archiver.archive_final(
                    req.content_length,
                    file_adler32,
                    file_md5.digest(),
                    segments,
                    EXCHANGE_TIMEOUT
                )
        # TODO: send space accounting message
        return Response('OK')
