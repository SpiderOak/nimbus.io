# -*- coding: utf-8 -*-
"""
Receives HTTP requests and distributes data to backend processes using zeromq

The web server uses gevent instead of the time queue event loop, so it has
some special modules to use gevent.

The web server has a GreenletResilientClient for each data writer

The resilient clients use Deliverator to deliver their messages.
"""
from gevent import monkey
# you must use the latest gevent and have c-ares installed for this to work
# with /etc/hosts 
# hg clone https://bitbucket.org/denis/gevent
monkey.patch_all()

import gevent_zeromq
gevent_zeromq.monkey_patch()

import gevent_psycopg2
gevent_psycopg2.monkey_patch()

import memcache

import logging
import os
import os.path
import pickle
import signal
import sys

from gevent.pywsgi import WSGIServer
from gevent.event import Event
import gevent.queue
from gevent_zeromq import zmq
import gevent

import gdbpool.interaction_pool

from tools.standard_logging import initialize_logging
from tools.greenlet_dealer_client import GreenletDealerClient
from tools.greenlet_resilient_client import GreenletResilientClient
from tools.greenlet_pull_server import GreenletPULLServer
from tools.deliverator import Deliverator
from tools.greenlet_push_client import GreenletPUSHClient
from tools.database_connection import get_central_database_dsn
from tools.event_push_client import EventPushClient
from tools.unified_id_factory import UnifiedIDFactory
from tools.id_translator import InternalIDTranslator
from tools.data_definitions import create_timestamp, \
        cluster_row_template, \
        node_row_template
from tools.interaction_pool_authenticator import \
    InteractionPoolAuthenticator
from tools.operational_stats_redis_sink import OperationalStatsRedisSink

from web_public_reader.space_accounting_client import SpaceAccountingClient

from web_writer.application import Application

class WebWriterError(Exception):
    pass

_log_path = "%s/nimbusio_web_writer_%s.log" % (
    os.environ["NIMBUSIO_LOG_DIR"], os.environ["NIMBUSIO_NODE_NAME"], )

_cluster_name = os.environ["NIMBUSIO_CLUSTER_NAME"]
_node_names = os.environ['NIMBUSIO_NODE_NAME_SEQ'].split()
_local_node_name = os.environ["NIMBUSIO_NODE_NAME"]
_client_tag = "web-writer-%s" % (_local_node_name, )
_web_writer_pipeliner_address = \
    os.environ["NIMBUSIO_WEB_WRITER_PIPELINE_ADDRESS"]
_data_writer_addresses = \
    os.environ["NIMBUSIO_DATA_WRITER_ADDRESSES"].split()
_space_accounting_server_address = \
    os.environ["NIMBUSIO_SPACE_ACCOUNTING_SERVER_ADDRESS"]
_space_accounting_pipeline_address = \
    os.environ["NIMBUSIO_SPACE_ACCOUNTING_PIPELINE_ADDRESS"]
_web_writer_host = os.environ.get("NIMBUSIO_WEB_WRITER_HOST", "")
_web_writer_port = int(os.environ["NIMBUSIO_WEB_WRITER_PORT"])
_wsgi_backlog = int(os.environ.get("NIMBUS_IO_WSGI_BACKLOG", "1024"))
_repository_path = os.environ["NIMBUSIO_REPOSITORY_PATH"]
_memcached_host = os.environ.get("NIMBUSIO_MEMCACHED_HOST", "localhost")
_memcached_port = int(os.environ.get("NIMBUSIO_MEMCACHED_PORT", "11211"))
_memcached_nodes = ["{0}:{1}".format(_memcached_host, _memcached_port), ]
_database_pool_size = 3 
_central_pool_name = "default"

def _signal_handler_closure(halt_event):
    def _signal_handler(*_args):
        halt_event.set()
    return _signal_handler

def _get_cluster_row_and_node_row(interaction_pool):
    """
    use node_id as shard id
    """
    log = logging.getLogger("_get_cluster_row_and_shard_id")

    query = """select %s from nimbusio_central.cluster where name = %%s""" % (\
        ",".join(cluster_row_template._fields), )

    async_result = interaction_pool.run(interaction=query, 
                                        interaction_args=[_cluster_name, ],
                                        pool=_central_pool_name) 
    result_list = async_result.get()

    if len(result_list) == 0:
        error_message = "Unable to identify cluster {0}".format(_cluster_name)
        log.error(error_message)
        raise WebWriterError(error_message)

    result = result_list[0]
    cluster_row = cluster_row_template(id=result["id"],
                                       name=result["name"],
                                       node_count=result["node_count"],
                                       replication_level=\
                                        result["replication_level"])

    query = """select %s from nimbusio_central.node
               where  cluster_id = %%s
               order by node_number_in_cluster""" % (
               ",".join(node_row_template._fields), )

    async_result = interaction_pool.run(interaction=query, 
                                        interaction_args=[cluster_row.id, ],
                                        pool=_central_pool_name) 
    result_list = async_result.get()

    for row in result_list:
        node_row = node_row_template(id=row["id"],
                                     node_number_in_cluster=\
                                        row["node_number_in_cluster"],
                                     name=row["name"],
                                     hostname=row["hostname"],
                                     offline=row["offline"])
        if node_row.name == _local_node_name:
            return cluster_row, node_row

    # if we make it here, this cluster is misconfigured
    error_message = "node name {0} is not in node cluster {1}".format(
            _local_node_name, _cluster_name)
    log.error(error_message)
    raise WebWriterError(error_message)

class WebWriter(object):
    def __init__(self, halt_event):
        self._log = logging.getLogger("WebWriter")
        memcached_client = memcache.Client(_memcached_nodes)

        self._interaction_pool = gdbpool.interaction_pool.DBInteractionPool(
            get_central_database_dsn(), 
            pool_name=_central_pool_name,
            pool_size=_database_pool_size, 
            do_log=True)

        authenticator = InteractionPoolAuthenticator(memcached_client, 
                                                     self._interaction_pool)

        # Ticket #25: must run database operation in a greenlet
        greenlet =  gevent.Greenlet.spawn(_get_cluster_row_and_node_row, 
                                           self._interaction_pool)
        greenlet.join()
        self._cluster_row, node_row = greenlet.get()

        self._unified_id_factory = UnifiedIDFactory(node_row.id)

        self._deliverator = Deliverator()

        self._zeromq_context = zmq.Context()

        self._pull_server = GreenletPULLServer(
            self._zeromq_context, 
            _web_writer_pipeliner_address,
            self._deliverator
        )
        self._pull_server.link_exception(self._unhandled_greenlet_exception)

        self._data_writer_clients = list()
        for node_name, address in zip(_node_names, _data_writer_addresses):
            resilient_client = GreenletResilientClient(
                self._zeromq_context, 
                node_name,
                address,
                _client_tag,
                _web_writer_pipeliner_address,
                self._deliverator,
                connect_messages=[]
            )
            resilient_client.link_exception(self._unhandled_greenlet_exception)
            self._data_writer_clients.append(resilient_client)

        self._space_accounting_dealer_client = GreenletDealerClient(
            self._zeromq_context, 
            _local_node_name, 
            _space_accounting_server_address
        )
        self._space_accounting_dealer_client.link_exception(
            self._unhandled_greenlet_exception
        )

        push_client = GreenletPUSHClient(
            self._zeromq_context, 
            _local_node_name, 
            _space_accounting_pipeline_address,
        )

        self._accounting_client = SpaceAccountingClient(
            _local_node_name,
            self._space_accounting_dealer_client,
            push_client
        )

        self._event_push_client = EventPushClient(
            self._zeromq_context,
            "web-server"
        )

        # message sent to data writers telling them the server
        # is (re)starting, thereby invalidating any archives
        # that are in progress for this node
        unified_id = self._unified_id_factory.next()
        timestamp = create_timestamp()
        self._event_push_client.info("web-writer-start",
                                     "web writer (re)start",
                                     unified_id=unified_id,
                                     timestamp_repr=repr(timestamp),
                                     source_node_name=_local_node_name)

        id_translator_keys_path = os.environ.get(
            "NIMBUS_IO_ID_TRANSLATION_KEYS", 
            os.path.join(_repository_path, "id_translator_keys.pkl"))
        with open(id_translator_keys_path, "r") as input_file:
            id_translator_keys = pickle.load(input_file)

        self._id_translator = InternalIDTranslator(
            id_translator_keys["key"],
            id_translator_keys["hmac_key"], 
            id_translator_keys["iv_key"],
            id_translator_keys["hmac_size"]
        )

        redis_queue = gevent.queue.Queue()

        self._redis_sink = OperationalStatsRedisSink(halt_event, redis_queue)
        self._redis_sink.link_exception(self._unhandled_greenlet_exception)

        self.application = Application(
            self._cluster_row,
            self._unified_id_factory,
            self._id_translator,
            self._data_writer_clients,
            authenticator,
            self._accounting_client,
            self._event_push_client,
            redis_queue
        )
        self.wsgi_server = WSGIServer((_web_writer_host, _web_writer_port), 
                                      application=self.application,
                                      backlog=_wsgi_backlog
        )

    def start(self):
        self._space_accounting_dealer_client.start()
        self._pull_server.start()
        for client in self._data_writer_clients:
            client.start()
        self._redis_sink.start()
        self.wsgi_server.start()

    def stop(self):
        self._log.info("stopping wsgi web server")
        self.wsgi_server.stop()
        self._accounting_client.close()
        self._log.debug("killing greenlets")
        self._space_accounting_dealer_client.kill()
        self._pull_server.kill()
        for client in self._data_writer_clients:
            client.kill()
        self._redis_sink.kill()
        self._log.debug("joining greenlets")
        self._space_accounting_dealer_client.join()
        self._pull_server.join()
        for client in self._data_writer_clients:
            client.join()
        self._redis_sink.kill()
        self._log.debug("closing zmq")
        self._event_push_client.close()
        self._zeromq_context.term()

    def _unhandled_greenlet_exception(self, greenlet_object):
        try:
            greenlet_object.get()
        except Exception:
            self._log.exception(str(greenlet_object))
            exctype, value = sys.exc_info()[:2]
            self._event_push_client.exception(
                "unhandled_greenlet_exception",
                str(value),
                exctype=exctype.__name__
            )

def main():
    initialize_logging(_log_path)
    log = logging.getLogger("main")
    halt_event = Event()
    gevent.signal(signal.SIGTERM, _signal_handler_closure(halt_event))

    try:
        web_writer = WebWriter(halt_event)
        web_writer.start()
    except Exception, instance:
        log.exception(str(instance))
        return -1

    halt_event.wait()
    log.info("halt_event set")

    try:
        web_writer.stop()
    except Exception, instance:
        log.exception(str(instance))
        return -1

    log.info("program terminates normally")
    return 0

if __name__ == '__main__':
    sys.exit(main(*sys.argv[1:]))
