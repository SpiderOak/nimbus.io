# -*- coding: utf-8 -*-
"""
application.py

The nimbus.io wsgi application
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

from tools.data_definitions import create_timestamp, nimbus_meta_prefix

from tools.collection import get_collection_from_hostname, \
        compute_default_collection_name, \
        create_collection, \
        list_collections, \
        delete_collection

from web_server.central_database_util import get_cluster_row
from web_server.exceptions import SpaceAccountingServerDownError, \
        SpaceUsageFailedError, \
        RetrieveFailedError, \
        ArchiveFailedError, \
        DestroyFailedError, \
        CollectionError
from web_server.data_writer_handoff_client import \
        DataWriterHandoffClient
from web_server.data_writer import DataWriter
from web_server.data_slicer import DataSlicer
from web_server.zfec_segmenter import ZfecSegmenter
from web_server.archiver import Archiver
from web_server.destroyer import Destroyer
from web_server.listmatcher import Listmatcher
from web_server.space_usage_getter import SpaceUsageGetter
from web_server.stat_getter import StatGetter
from web_server.retriever import Retriever
from web_server.meta_manager import get_meta, list_meta
from web_server.conjoined_manager import list_conjoined_archives, \
        start_conjoined_archive, \
        abort_conjoined_archive, \
        finish_conjoined_archive

_node_names = os.environ['NIMBUSIO_NODE_NAME_SEQ'].split()
_reply_timeout = float(
    os.environ.get("NIMBUSIO_REPLY_TIMEOUT",  str(5 * 60.0))
)
_slice_size = 1024 * 1024    # 1MB
_min_connected_clients = 8
_min_segments = 8
_max_segments = 10
_handoff_count = 2

_s3_meta_prefix = "x-amz-meta-"
_sizeof_s3_meta_prefix = len(_s3_meta_prefix)

def _build_meta_dict(req_get):
    """
    create a dict of meta values, conveting the aws prefix to ours
    """
    meta_dict = dict()
    for key in req_get:
        if key.startswith(_s3_meta_prefix):
            converted_key = "".join(
                    [nimbus_meta_prefix, key[_sizeof_s3_meta_prefix:]]
                )
            meta_dict[converted_key] = req_get[key]
        elif key.startswith(nimbus_meta_prefix):
            meta_dict[key] = req_get[key]

    return meta_dict

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

    for node_name, client in zip(_node_names, clients):
        if client.connected:
            connected_clients_by_node.append((node_name, client))
        else:
            disconnected_clients_by_node.append((node_name, client))

    if len(connected_clients_by_node) < _min_connected_clients:
        raise exc.HTTPServiceUnavailable("Too few connected writers %s" % (
            len(connected_clients_by_node),
        ))

    connected_clients = list()
    for node_name, client in connected_clients_by_node:
        connected_clients.append(client)
        assert node_name not in data_writers_dict, connected_clients_by_node
        data_writers_dict[node_name] = DataWriter(node_name, client)
    
    for node_name, client in disconnected_clients_by_node:
        backup_clients = random.sample(connected_clients, _handoff_count)
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
    # the same order as _node_names, because that's the order that
    # segment numbers get defined in
    return [data_writers_dict[node_name] for node_name in _node_names]

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
        self._central_connection = central_connection
        self._node_local_connection = node_local_connection
        self._data_writer_clients = data_writer_clients
        self.data_readers = data_readers
        self._authenticator = authenticator
        self.accounting_client = accounting_client
        self._event_push_client = event_push_client

        self._cluster_row = get_cluster_row(self._central_connection)

    routes = router()

    @wsgify
    def __call__(self, req):
        # TODO: test this
        try:
            collection_entry = get_collection_from_hostname(
                self._central_connection, req.host
            )
        except Exception, instance:
            self._log.error("%s" % (instance, ))
            raise exc.HTTPBadRequest()
            
        authenticated = self._authenticator.authenticate(
            self._central_connection,
            collection_entry.username,
            req
        )
        if not authenticated:
            raise exc.HTTPUnauthorized()

        try:
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
                        collection_entry, 
                        req, 
                        *url_match.groups(), 
                        **url_match.groupdict()
                    )
                    return result
                except exc.HTTPException, instance:
                    self._log.error("%s %s %s" % (
                        instance.__class__.__name__, 
                        instance, 
                        collection_entry,
                    ))
                    raise
                except Exception, instance:
                    self._log.exception("%s" % (collection_entry, ))
                    self._event_push_client.exception(
                        "unhandled_exception",
                        str(instance),
                        exctype=instance.__class__.__name__
                    )
                    raise

            if url_matched:
                raise exc.HTTPMethodNotAllowed()
            raise exc.HTTPNotFound(req.path)
        except:
            self._log.exception('error in __call__')
            raise

    @routes.add(r'/usage$')
    def usage(self, collection_entry, _req):
        self._log.debug("usage: %r %r" % (
            collection_entry.username, collection_entry.collection_name
        ))

        getter = SpaceUsageGetter(self.accounting_client)
        try:
            usage = getter.get_space_usage(
                collection_entry.collection_id, _reply_timeout
            )
        except (SpaceAccountingServerDownError, SpaceUsageFailedError), e:
            raise exc.HTTPServiceUnavailable(str(e))

        return Response(json.dumps(usage))

    @routes.add(r'/data/(.+)$', action='stat')
    def stat(self, collection_entry, _req, path):
        try:
            key = urllib.unquote_plus(path)
            key = key.decode("utf-8")
        except Exception, instance:
            raise exc.HTTPServiceUnavailable(str(instance))

        self._log.debug("stat: collection = (%s) %r username = %r key = %r" % (
            collection_entry.collection_id, 
            collection_entry.collection_name,
            collection_entry.username,
            key
        ))

        getter = StatGetter(self._node_local_connection)
        file_info = getter.stat(
            collection_entry.collection_id, key, _reply_timeout
        )
        if file_info is None or file_info.file_tombstone:
            raise exc.HTTPNotFound("Not Found: %r" % (key, ))

        file_info_dict = dict()
        for key, value in file_info._asdict().items():
            if key.startswith("file_") and key != "file_tombstone":
                if key == "file_hash" and value is not None:
                    value = hexlify(value)
                file_info_dict[key] = value

        return Response(json.dumps(file_info_dict))

    @routes.add(r'/data/(.*)$', action='listmatch')
    def listmatch(self, collection_entry, _req, prefix):
        try:
            prefix = urllib.unquote_plus(prefix)
            prefix = prefix.decode("utf-8")
        except Exception, instance:
            raise exc.HTTPServiceUnavailable(str(instance))

        self._log.debug(
            "listmatch: collection = (%s) username = %r %r prefix = '%s'" % (
                collection_entry.collection_id,
                collection_entry.collection_name,
                collection_entry.username,
                prefix
            )
        )
        matcher = Listmatcher(self._node_local_connection)
        keys = matcher.listmatch(
            collection_entry.collection_id, prefix, _reply_timeout
        )
        response = Response(content_type='text/plain', charset='utf8')
        response.body_file.write(json.dumps(keys))
        return response

    @routes.add(r'/create_collection$')
    def create_collection(self, collection_entry, req):
        self._log.debug("create_collection: %s name = %r" % (
            collection_entry.username,
            req.GET["collection_name"],
        ))

        try:
            create_collection(
                self._central_connection, 
                collection_entry.username,
                req.GET["collection_name"]
            )
        except Exception, instance:
            self._log.error("%s error adding collection %r %s" % (
                collection_entry.username, 
                req.GET["collection_name"], 
                instance,
            ))
            self._central_connection.rollback()
            raise exc.HTTPServiceUnavailable(str(instance))
        else:
            self._central_connection.commit()

        return Response('OK')

    @routes.add(r'/list_collections')
    def list_collections(self, collection_entry, _req):
        self._log.debug("%s list_collections" % (collection_entry.username, ))
        try:
            collections = list_collections(
                self._central_connection,
                collection_entry.username
            )
        except Exception, instance:
            self._log.error("%s error listing collections %s" % (
                collection_entry, instance,
            ))
            raise exc.HTTPServiceUnavailable(str(instance))

        # json won't dump datetime
        json_collections = [(n, t.isoformat()) for (n, t) in collections]

        response = Response(content_type='text/plain', charset='utf8')
        response.body_file.write(json.dumps(json_collections))

        return response

    @routes.add(r'/delete_collection$')
    def delete_collection(self, collection_entry, req):
        collection_name = req.GET["collection_name"]
        self._log.debug("delete_collection: %r  %s" % (
            collection_name, collection_entry, 
        ))

        # you can't delete your default collection
        default_collection_name = compute_default_collection_name(
            collection_entry.username
        )
        if collection_name == default_collection_name:
            raise exc.HTTPForbidden("Can't delete default collection %r" % (
                collection_name,
            ))

        # TODO: can't delete a collection that contains keys
        try:
            delete_collection(self._central_connection, collection_name)
        except Exception, instance:
            self._log.error("%s error deleting collection %s" % (
                collection_entry, instance,
            ))
            self._central_connection.rollback()
            raise exc.HTTPServiceUnavailable(str(instance))
        else:
            self._central_connection.commit()

        return Response('OK')

    @routes.add(r'/data/(.+)$', 'DELETE')
    @routes.add(r'/data/(.+)$', 'POST', action='delete')
    def destroy(self, collection_entry, _req, key):
        try:
            key = urllib.unquote_plus(key)
            key = key.decode("utf-8")
        except Exception, instance:
            raise exc.HTTPServiceUnavailable(str(instance))

        self._log.debug(
            "destroy: collection = (%s) %r customer = %r key = %r" % (
                collection_entry.collection_id,
                collection_entry.collection_name,
                collection_entry.username,
                key,
        ))
        data_writers = _create_data_writers(self._data_writer_clients)

        timestamp = create_timestamp()

        destroyer = Destroyer(
            self._node_local_connection,
            data_writers,
            collection_entry.collection_id,
            key,
            timestamp
        )

        try:
            size_deleted = destroyer.destroy(_reply_timeout)
        except DestroyFailedError, e:            
            raise exc.HTTPInternalServerError(str(e))

        self.accounting_client.removed(
            collection_entry.collection_id,
            timestamp,
            size_deleted
        )
        return Response('OK')

    @routes.add(r'/data/(.+)$', action="get_meta")
    def get_meta(self, collection_entry, req, key):
        try:
            key = urllib.unquote_plus(key)
            key = key.decode("utf-8")
        except Exception, instance:
            self._log.error('unable to prepare key %r %s' % (
                key, instance
            ))
            raise exc.HTTPServiceUnavailable(str(instance))

        meta_value = get_meta(
            self._node_local_connection,
            collection_entry.collection_id,
            key,
            req.GET["meta_key"]
        )

        if meta_value is None:
            raise exc.HTTPNotFound(req.GET["meta_key"])

        response = Response(content_type='text/plain', charset='utf8')
        response.body_file.write(meta_value)

        return response

    @routes.add(r'/data/(.+)$', action="list_meta")
    def list_meta(self, collection_entry, _req, key):
        try:
            key = urllib.unquote_plus(key)
            key = key.decode("utf-8")
        except Exception, instance:
            self._log.error('unable to prepare key %r %s' % (
                key, instance
            ))
            raise exc.HTTPServiceUnavailable(str(instance))

        meta_value = list_meta(
            self._node_local_connection,
            collection_entry.collection_id,
            key
        )

        response = Response(content_type='text/plain', charset='utf8')
        response.body_file.write(json.dumps(meta_value))

        return response

    @routes.add(r"/list_conjoined_archives")
    def list_conjoined_archives(self, collection_entry, _req):
        conjoined_value = list_conjoined_archives(
            self._central_connection,
            collection_entry.collection_id,
        )

        response = Response(content_type='text/plain', charset='utf8')
        response.body_file.write(json.dumps(conjoined_value))

        return response

    @routes.add(r'/data/(.+)$', action="start_conjoined_archive")
    def start_conjoined_archive(self, collection_entry, _req, key):
        try:
            key = urllib.unquote_plus(key)
            key = key.decode("utf-8")
        except Exception, instance:
            self._log.error('unable to prepare key %r %s' % (
                key, instance
            ))
            raise exc.HTTPServiceUnavailable(str(instance))

        conjoined_identifier = start_conjoined_archive(
            self._central_connection,
            collection_entry.collection_id,
            key
        )

        response = Response(content_type='text/plain', charset='utf8')
        response.body_file.write(conjoined_identifier)

        return response

    @routes.add(r'/data/(.+)$')
    def retrieve(self, collection_entry, _req, key):
        start_time = time.time()

        try:
            key = urllib.unquote_plus(key)
            key = key.decode("utf-8")
        except Exception, instance:
            raise exc.HTTPServiceUnavailable(str(instance))

        description = "retrieve: collection=(%s)%r customer=%r key=%r" % (
            collection_entry.collection_id,
            collection_entry.collection_name,
            collection_entry.username,
            key
        )
        self._log.debug(description)
        connected_data_readers = _connected_clients(self.data_readers)

        if len(connected_data_readers) < _min_connected_clients:
            raise exc.HTTPServiceUnavailable("Too few connected readers %s" % (
                len(connected_data_readers),
            ))

        segmenter = ZfecSegmenter(
            _min_segments,
            _max_segments)
        retriever = Retriever(
            self._node_local_connection,
            self.data_readers,
            collection_entry.collection_id,
            key,
            _min_segments
        )

        retrieved = retriever.retrieve(_reply_timeout)

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
                collection_entry.collection_id,
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
    def archive(self, collection_entry, req, key):
        try:
            key = urllib.unquote_plus(key)
            key = key.decode("utf-8")
        except Exception, instance:
            self._log.error('unable to prepare key %r %s' % (
                key, instance
            ))
            raise exc.HTTPServiceUnavailable(str(instance))

        start_time = time.time()
        key = urllib.unquote_plus(key)
        description = \
                "archive: collection=(%s)%r customer=%r key=%r, size=%s" % (
            collection_entry.collection_id,
            collection_entry.collection_name,
            collection_entry.username,
            key, 
            req.content_length
        )
        self._log.debug(description)

        if req.content_length <= 0:
            raise exc.HTTPForbidden(
                "cannot archive: content_length = %s" % (req.content_length, )
            ) 

        meta_dict = _build_meta_dict(req.GET)

        data_writers = _create_data_writers(self._data_writer_clients) 
        timestamp = create_timestamp()
        archiver = Archiver(
            data_writers,
            collection_entry.collection_id,
            key,
            timestamp,
            meta_dict
        )
        segmenter = ZfecSegmenter(
            8,
            len(data_writers)
        )
        file_adler32 = zlib.adler32('')
        file_md5 = hashlib.md5()
        file_size = 0
        segments = None
        try:
            for slice_item in DataSlicer(req.body_file,
                                    _slice_size,
                                    req.content_length):
                if segments:
                    archiver.archive_slice(
                        segments,
                        _reply_timeout
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
                segments,
                _reply_timeout
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
            collection_entry.collection_id,
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

