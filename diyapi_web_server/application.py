# -*- coding: utf-8 -*-
"""
application.py

The diyapi wsgi application
"""
import logging
import os
import re
import random
import zlib
import hashlib
import json
from itertools import chain
from binascii import hexlify
import urllib
import time

from webob.dec import wsgify
from webob import exc
from webob import Response

from diyapi_tools.data_definitions import create_timestamp

from diyapi_web_server import util
from diyapi_web_server.central_database_util import get_cluster_row, \
        get_collections_for_avatar 
from diyapi_web_server.exceptions import SpaceAccountingServerDownError, \
        SpaceUsageFailedError, \
        RetrieveFailedError, \
        ArchiveFailedError, \
        DestroyFailedError
from diyapi_web_server.data_writer_handoff_client import \
        DataWriterHandoffClient
from diyapi_web_server.data_writer import DataWriter
from diyapi_web_server.data_slicer import DataSlicer
from diyapi_web_server.zfec_segmenter import ZfecSegmenter
from diyapi_web_server.archiver import Archiver
from diyapi_web_server.destroyer import Destroyer
from diyapi_web_server.listmatcher import Listmatcher
from diyapi_web_server.space_usage_getter import SpaceUsageGetter
from diyapi_web_server.stat_getter import StatGetter
from diyapi_web_server.retriever import Retriever

NODE_NAMES = os.environ['SPIDEROAK_MULTI_NODE_NAME_SEQ'].split()
REPLY_TIMEOUT = float(
    os.environ.get("SPIDEROAK_DIYAPI_REPLY_TIMEOUT",  str(5 * 60.0))
)
SLICE_SIZE = 1024 * 1024    # 1MB
MIN_CONNECTED_CLIENTS = 8
MIN_SEGMENTS = 8
MAX_SEGMENTS = 10
HANDOFF_COUNT = 2

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

def _connected_clients(clients):
    return [client for client in clients if client.connected]

def _create_data_writers(clients):
    data_writers_dict = dict()

    connected_clients_by_node = list()
    disconnected_clients_by_node = list()

    for node_name, client in zip(NODE_NAMES, clients):
        if client.connected:
            connected_clients_by_node.append((node_name, client))
        else:
            disconnected_clients_by_node.append((node_name, client))

    if len(connected_clients_by_node) < MIN_CONNECTED_CLIENTS:
        raise exc.HTTPServiceUnavailable("Too few connected writers %s" % (
            len(connected_clients_by_node),
        ))

    connected_clients = list()
    for node_name, client in connected_clients_by_node:
        connected_clients.append(client)
        assert node_name not in data_writers_dict, connected_clients_by_node
        data_writers_dict[node_name] = DataWriter(node_name, client)
    
    for node_name, client in disconnected_clients_by_node:
        backup_clients = random.sample(connected_clients, HANDOFF_COUNT)
        assert backup_clients[0] != backup_clients[1]
        data_writer_handoff_client = DataWriterHandoffClient(
            client.server_node_name,
            backup_clients
        )
        assert node_name not in data_writers_dict, data_writers_dict
        data_writers_dict[node_name] = DataWriter(
            node_name, data_writer_handoff_client
        )

    # 2011-05-27 dougfort -- the data-writers list must be in 
    # the same order as NODE_NAMES, because that's the order that
    # segment numbers get defined in
    return [data_writers_dict[node_name] for node_name in NODE_NAMES]

class Application(object):
    def __init__(
        self, 
        central_connection,
        node_local_connection,
        data_writer_clients, 
        data_readers,
        authenticator, 
        accounting_client,
        event_push_client
    ):
        self._log = logging.getLogger("Application")
        self._data_writer_clients = data_writer_clients
        self._central_connection = central_connection
        self._node_local_connection = node_local_connection
        self.data_readers = data_readers
        self.authenticator = authenticator
        self.accounting_client = accounting_client
        self._event_push_client = event_push_client

        self._cluster_row = get_cluster_row(self._central_connection)

    routes = router()

    @wsgify
    def __call__(self, req):
        # TODO: test this
        try:
            req.diy_username = util.get_username_from_req(req) or 'test'
            if not self.authenticator.authenticate(req):
                raise exc.HTTPUnauthorized()
            req.collections = dict(
                get_collections_for_avatar(
                    self._central_connection,
                    self._cluster_row.id,
                    req.avatar_id
                )
            )
            req.default_collection_id = req.collections["(default)"]
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

                try:
                    result = method(
                        req, *url_match.groups(), **url_match.groupdict()
                    )
                    return result
                except Exception:
                    self._log.exception("%s" % (req.diy_username, ))
                    raise

            if url_matched:
                raise exc.HTTPMethodNotAllowed()
            raise exc.HTTPNotFound(req.path)
        except:
            self._log.exception('error in __call__')
            raise

    @routes.add(r'/usage$')
    def usage(self, req):
        self._log.debug("usage: %s" % (req.avatar_id, ))
        getter = SpaceUsageGetter(self.accounting_client)
        try:
            usage = getter.get_space_usage(req.avatar_id, REPLY_TIMEOUT)
        except (SpaceAccountingServerDownError, SpaceUsageFailedError), e:
            raise exc.HTTPServiceUnavailable(str(e))
        return Response(json.dumps(usage))

    @routes.add(r'/data/(.+)$', action='stat')
    def stat(self, req, path):
        # TODO: get the collection name from the uri
        collection_name = "(default)"
        collection_id = req.collections[collection_name]
        key = urllib.unquote_plus(path)
        self._log.debug("stat: %s collection = (%s) %r key = %r" % (
            req.avatar_id,
            collection_id, 
            collection_name,
            key
        ))
        getter = StatGetter(self._node_local_connection)
        file_info = getter.stat(collection_id, key, REPLY_TIMEOUT)
        file_info_dict = dict()
        for key, value in file_info._asdict().items():
            if key.startswith("file_"):
                if key == "file_hash" and value is not None:
                    value = hexlify(value)
                file_info_dict[key] = value
        return Response(json.dumps(file_info_dict))

    @routes.add(r'/data/(.*)$', action='listmatch')
    def listmatch(self, req, prefix):
        # TODO: get the collection name from the uri
        collection_name = "(default)"
        collection_id = req.collections[collection_name]
        prefix = urllib.unquote_plus(prefix)
        self._log.debug("listmatch: %s collection = (%s) %r prefix = '%s'" % (
            req.avatar_id, 
            collection_id,
            collection_name,
            prefix
        ))
        matcher = Listmatcher(self._node_local_connection)
        keys = matcher.listmatch(collection_id, prefix, REPLY_TIMEOUT)
        response = Response(content_type='text/plain', charset='utf8')
        for key in keys:
            response.body_file.write("%s\n" % (key, ))
        return response

    @routes.add(r'/data/(.+)$', 'DELETE')
    @routes.add(r'/data/(.+)$', 'POST', action='delete')
    def destroy(self, req, key):
        # TODO: get the collection name from the uri
        collection_name = "(default)"
        collection_id = req.collections[collection_name]
        key = urllib.unquote_plus(key)
        self._log.debug("destroy: %s collection = (%s) %r key = %s" % (
            req.avatar_id, 
            collection_id,
            collection_name,
            key,
        ))
        data_writers = _create_data_writers(self._data_writer_clients)

        timestamp = create_timestamp()

        destroyer = Destroyer(
            self._node_local_connection,
            data_writers,
            collection_id,
            key,
            timestamp
        )

        try:
            size_deleted = destroyer.destroy(REPLY_TIMEOUT)
        except DestroyFailedError, e:            
            raise exc.HTTPInternalServerError(str(e))

        self.accounting_client.removed(
            req.avatar_id,
            timestamp,
            size_deleted
        )
        return Response('OK')

    @routes.add(r'/data/(.+)$')
    def retrieve(self, req, key):
        # TODO: get the collection name from the uri
        collection_name = "(default)"
        collection_id = req.collections[collection_name]
        start_time = time.time()
        key = urllib.unquote_plus(key)
        description = "retrieve: %s collection = (%s) %r key = %r" % (
            req.avatar_id, 
            collection_id,
            collection_name,
            key
        )
        self._log.debug(description)
        connected_data_readers = _connected_clients(self.data_readers)

        if len(connected_data_readers) < MIN_CONNECTED_CLIENTS:
            raise exc.HTTPServiceUnavailable("Too few connected readers %s" % (
                len(connected_data_readers),
            ))

        segmenter = ZfecSegmenter(
            MIN_SEGMENTS,
            MAX_SEGMENTS)
        retriever = Retriever(
            self._node_local_connection,
            self.data_readers,
            collection_id,
            key,
            MIN_SEGMENTS
        )

        retrieved = retriever.retrieve(REPLY_TIMEOUT)

        try:
            first_segments = retrieved.next()
        except RetrieveFailedError, instance:
            self._log.error("retrieve failed: %s %s" % (
                description, instance,
            ))
            self._event_push_client.error(
                "retrieve-failed",
                "%s: %s" % (description, instance, )
            )
            return exc.HTTPNotFound(str(instance))

        def app_iter():
            sent = 0
            try:
                for segments in chain([first_segments], retrieved):
                    data = segmenter.decode(segments.values())
                    sent += len(data)
                    yield data
            except RetrieveFailedError, instance:
                self._event_push_client.error(
                    "retrieve-failed",
                    "%s: %s" % (description, instance, )
                )
                self._log.error('retrieve failed: %s %s' % (
                    description, instance
                ))
                raise exc.HTTPInternalServerError(str(instance))

            end_time = time.time()

            self.accounting_client.retrieved(
                req.avatar_id,
                create_timestamp(),
                sent
            )

            self._event_push_client.info(
                "retrieve-stats",
                description,
                start_time=start_time,
                end_time=end_time,
                bytes_retrieved=sent
            )

        return Response(app_iter=app_iter())

    @routes.add(r'/data/(.+)$', 'POST')
    def archive(self, req, key):
        # TODO: get the collection name from the uri
        collection_name = "(default)"
        collection_id = req.collections[collection_name]
        start_time = time.time()
        key = urllib.unquote_plus(key)
        description = "archive: %s collection = (%s) %r key = %r, size=%s" % (
            req.avatar_id, 
            collection_id,
            collection_name,
            key, 
            req.content_length
        )
        self._log.debug(description)

        if req.content_length <= 0:
            raise exc.HTTPForbidden(
                "cannot archive: content_length = %s" % (req.content_length, )
            ) 

        data_writers = _create_data_writers(self._data_writer_clients) 
        timestamp = create_timestamp()
        archiver = Archiver(
            data_writers,
            collection_id,
            key,
            timestamp
        )
        segmenter = ZfecSegmenter(
            8,
            len(data_writers)
        )
        file_adler32 = zlib.adler32('')
        file_md5 = hashlib.md5()
        file_size = 0
        # TODO: get these file attributs from somewhere
        file_user_id = None
        file_group_id = None
        file_permissions = None
        segments = None
        try:
            for slice_item in DataSlicer(req.body_file,
                                    SLICE_SIZE,
                                    req.content_length):
                if segments:
                    archiver.archive_slice(
                        segments,
                        REPLY_TIMEOUT
                    )
                    segments = None
                file_adler32 = zlib.adler32(slice_item, file_adler32)
                file_md5.update(slice_item)
                file_size += len(slice_item)
                segments = segmenter.encode(slice_item)
            if not segments:
                segments = segmenter.encode('')
            archiver.archive_final(
                file_size,
                file_adler32,
                file_md5.digest(),
                file_user_id,
                file_group_id,
                file_permissions,
                segments,
                REPLY_TIMEOUT
            )
        except ArchiveFailedError, instance:
            self._event_push_client.error(
                "archived-failed-error",
                "%s: %s" % (description, instance, )
            )
            self._log.error("archive failed: %s %s" % (
                description, instance, 
            ))
            raise exc.HTTPInternalServerError(str(instance))
        
        end_time = time.time()

        self.accounting_client.added(
            req.avatar_id,
            timestamp,
            file_size
        )

        self._event_push_client.info(
            "archive-stats",
            description,
            start_time=start_time,
            end_time=end_time,
            bytes_archived=req.content_length
        )

        return Response('OK')

