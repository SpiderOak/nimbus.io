# -*- coding: utf-8 -*-
"""
application.py

The diyapi wsgi application
"""
import logging
import os
import re
import random
import time
import zlib
import hashlib
import json
from itertools import chain
from binascii import hexlify

from webob.dec import wsgify
from webob import exc
from webob import Response

from diyapi_web_server import util
from diyapi_web_server.exceptions import SpaceAccountingServerDownError, \
        SpaceUsageFailedError, \
        DataReaderDownError, \
        StatFailedError, \
        DataReaderDownError, \
        ListmatchFailedError, \
        DataWriterDownError, \
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
DATABASE_AGREEMENT_LEVEL = 8
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

def _create_data_writers(clients, handoff_client):
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
            backup_clients,
            handoff_client
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
        data_writer_clients, 
        handoff_client,
        data_readers,
        database_clients, 
        authenticator, 
        accounting_client
    ):
        self._log = logging.getLogger("Application")
        self._data_writer_clients = data_writer_clients
        self._handoff_client = handoff_client
        self.data_readers = data_readers
        self.database_clients = database_clients
        self.authenticator = authenticator
        self.accounting_client = accounting_client

    routes = router()

    @wsgify
    def __call__(self, req):
        # TODO: test this
        try:
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
            raise exc.HTTPNotFound()
        except:
            self._log.exception('error in __call__')
            raise

    @routes.add(r'/usage$')
    def usage(self, req):
        self._log.debug("usage: avatar_id = %s" % (
            req.remote_user,
        ))
        avatar_id = req.remote_user
        getter = SpaceUsageGetter(self.accounting_client)
        try:
            usage = getter.get_space_usage(avatar_id, REPLY_TIMEOUT)
        except (SpaceAccountingServerDownError, SpaceUsageFailedError), e:
            raise exc.HTTPServiceUnavailable(str(e))
        return Response(json.dumps(usage))

    @routes.add(r'/data/(.+)$', action='stat')
    def stat(self, req, path):
        self._log.debug("stat: avatar_id = %s path = %r" % (
            req.remote_user,
            path
        ))
        connected_database_clients = _connected_clients(self.database_clients)

        if len(connected_database_clients) < MIN_CONNECTED_CLIENTS:
            raise exc.HTTPServiceUnavailable("Too few connected clients %s" % (
                len(connected_database_clients),
            ))

        avatar_id = req.remote_user
        getter = StatGetter(
            connected_database_clients,
            DATABASE_AGREEMENT_LEVEL
        )
        try:
            stat = getter.stat(avatar_id, path, REPLY_TIMEOUT)
        except (DataReaderDownError, StatFailedError):
            self._log.exception(path)
            raise exc.HTTPNotFound()
        if 'file_md5' in stat:
            stat['file_md5'] = hexlify(stat['file_md5'])
        return Response(json.dumps(stat))

    @routes.add(r'/data/(.*)$', action='listmatch')
    def listmatch(self, req, prefix):
        self._log.debug("listmatch: avatar_id = %s prefix = '%s'" % (
            req.remote_user, prefix
        ))
        connected_database_clients = _connected_clients(self.database_clients)

        if len(connected_database_clients) < MIN_CONNECTED_CLIENTS:
            raise exc.HTTPServiceUnavailable("Too few connected clients %s" % (
                len(connected_database_clients),
            ))

        delimiter = req.GET.get('delimiter', '/')
        # TODO: do something with delimiter
        avatar_id = req.remote_user
        matcher = Listmatcher(
            connected_database_clients,
            DATABASE_AGREEMENT_LEVEL
        )
        try:
            keys = matcher.listmatch(avatar_id, prefix, REPLY_TIMEOUT)
        except (DataReaderDownError, ListmatchFailedError), e:
            raise exc.HTTPInternalServerError(str(e))
        # TODO: break up large (>1mb) listmatch response
        return Response(json.dumps(keys))

    @routes.add(r'/data/(.+)$', 'DELETE')
    @routes.add(r'/data/(.+)$', 'POST', action='delete')
    def destroy(self, req, key):
        self._log.debug("destroy: avatar_id = %s key = %s" % (
            req.remote_user, key,
        ))
        data_writers = _create_data_writers(
            self._data_writer_clients, self._handoff_client
        )

        avatar_id = req.remote_user
        timestamp = time.time()
        destroyer = Destroyer(data_writers)
        try:
            size_deleted = destroyer.destroy(
                avatar_id, key, timestamp, REPLY_TIMEOUT)
        except (DataWriterDownError, DestroyFailedError), e:
            raise exc.HTTPInternalServerError(str(e))
        self.accounting_client.removed(
            avatar_id,
            timestamp,
            size_deleted
        )
        return Response('OK')

    @routes.add(r'/data/(.+)$')
    def retrieve(self, req, key):
        self._log.debug("retrieve: avatar_id = %s key = %s" % (
            req.remote_user, key
        ))
        connected_data_readers = _connected_clients(self.data_readers)

        if len(connected_data_readers) < MIN_CONNECTED_CLIENTS:
            raise exc.HTTPServiceUnavailable("Too few connected readers %s" % (
                len(connected_data_readers),
            ))

        avatar_id = req.remote_user
        timestamp = time.time()
        segmenter = ZfecSegmenter(
            MIN_SEGMENTS,
            MAX_SEGMENTS)
        retriever = Retriever(
            self.data_readers,
            avatar_id,
            key,
            MIN_SEGMENTS
        )
        retrieved = retriever.retrieve(REPLY_TIMEOUT)
        try:
            first_segments = retrieved.next()
        except (DataWriterDownError, RetrieveFailedError):
            return exc.HTTPNotFound()
        def app_iter():
            sent = 0
            try:
                for segments in chain([first_segments], retrieved):
                    data = segmenter.decode(segments.values())
                    sent += len(data)
                    yield data
            except (DataWriterDownError, RetrieveFailedError):
                self._log.warning('retrieve failed: avatar_id = %s' % (
                    avatar_id,
                ))
            self.accounting_client.retrieved(
                avatar_id,
                timestamp,
                sent
            )
        return Response(app_iter=app_iter())

    @routes.add(r'/data/(.+)$', 'POST')
    def archive(self, req, key):
        self._log.debug(
            "archive: avatar_id = %s key = %s, content_length = %s" % (
                req.remote_user, key, req.content_length
            )
        )

        if req.content_length <= 0:
            raise exc.HTTPForbidden(
                "cannot archive: content_length = %s" % (req.content_length, )
            ) 

        data_writers = _create_data_writers(
            self._data_writer_clients, self._handoff_client
        )

        avatar_id = req.remote_user
        timestamp = time.time()
        archiver = Archiver(
            data_writers,
            avatar_id,
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
        previous_size = 0
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
            previous_size = archiver.archive_final(
                file_size,
                file_adler32,
                file_md5.digest(),
                segments,
                REPLY_TIMEOUT
            )
        except (ArchiveFailedError), e:
            raise exc.HTTPInternalServerError(str(e))
        if previous_size is not None:
            self.accounting_client.removed(
                avatar_id,
                timestamp,
                previous_size
            )
        self.accounting_client.added(
            avatar_id,
            timestamp,
            file_size
        )
        return Response('OK')

