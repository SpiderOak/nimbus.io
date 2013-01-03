# -*- coding: utf-8 -*-
"""
Receives HTTP requests and distributes data to backend processes using zeromq

The web server uses gevent instead of the time queue event loop, so it has
some special modules to use gevent.
"""
from gevent.monkey import patch_all
# you must use the latest gevent and have c-ares installed for this to work
# with /etc/hosts 
# hg clone https://bitbucket.org/denis/gevent
patch_all()

import gevent_zeromq
gevent_zeromq.monkey_patch()

import gevent_psycopg2
gevent_psycopg2.monkey_patch()

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
from tools.greenlet_push_client import GreenletPUSHClient
from tools.database_connection import get_central_database_dsn, \
        get_node_local_database_dsn
from tools.event_push_client import EventPushClient
from tools.id_translator import InternalIDTranslator
from tools.interaction_pool_authenticator import \
    InteractionPoolAuthenticator
from tools.data_definitions import cluster_row_template
from tools.operational_stats_redis_sink import OperationalStatsRedisSink

from web_public_reader.memcached_client import create_memcached_client
from web_public_reader.application import Application
from web_public_reader.space_accounting_client import SpaceAccountingClient

class WebPublicReaderError(Exception):
    pass

_log_path = "%s/nimbusio_web_public_reader_%s.log" % (
    os.environ["NIMBUSIO_LOG_DIR"], os.environ["NIMBUSIO_NODE_NAME"], )

_cluster_name = os.environ["NIMBUSIO_CLUSTER_NAME"]
_local_node_name = os.environ["NIMBUSIO_NODE_NAME"]
_space_accounting_server_address = \
    os.environ["NIMBUSIO_SPACE_ACCOUNTING_SERVER_ADDRESS"]
_space_accounting_pipeline_address = \
    os.environ["NIMBUSIO_SPACE_ACCOUNTING_PIPELINE_ADDRESS"]
_web_public_reader_host = os.environ.get("NIMBUSIO_WEB_PUBLIC_READER_HOST", "")
_web_public_reader_port = \
    int(os.environ.get("NIMBUSIO_WEB_PUBLIC_READER_PORT", "8088"))
_wsgi_backlog = int(os.environ.get("NIMBUS_IO_WSGI_BACKLOG", "1024"))
_repository_path = os.environ["NIMBUSIO_REPOSITORY_PATH"]
_central_database_pool_size = 3 
_central_pool_name = "default"
_local_database_pool_size = 3 

def _signal_handler_closure(halt_event):
    def _signal_handler(*_args):
        halt_event.set()
    return _signal_handler

def _get_cluster_row(interaction_pool):
    """
    use node_id as shard id
    """
    log = logging.getLogger("_get_cluster_row_and_shard_id")

    query = """select %s from nimbusio_central.cluster where name = %%s""" % (\
        ",".join(cluster_row_template._fields), )

    async_result = \
        interaction_pool.run(interaction=query, 
                             interaction_args=[_cluster_name, ],
                             pool=_central_pool_name) 
    result_list = async_result.get()

    if len(result_list) == 0:
        error_message = "Unable to identify cluster {0}".format(_cluster_name)
        log.error(error_message)
        raise WebPublicReaderError(error_message)

    result = result_list[0]
    return cluster_row_template(id=result["id"],
                                name=result["name"],
                                node_count=result["node_count"],
                                replication_level=result["replication_level"])

class WebPublicReaderServer(object):
    def __init__(self, halt_event):
        self._log = logging.getLogger("WebServer")
        memcached_client = create_memcached_client()

        self._interaction_pool = \
            gdbpool.interaction_pool.DBInteractionPool(
                get_central_database_dsn(), 
                pool_name=_central_pool_name,
                pool_size=_central_database_pool_size, 
                do_log=True)

        self._interaction_pool.add_pool(
            dsn=get_node_local_database_dsn(), 
            pool_name=_local_node_name,
            pool_size=_local_database_pool_size) 

        # Ticket #25: must run database operation in a greenlet
        greenlet =  gevent.Greenlet.spawn(_get_cluster_row, 
                                           self._interaction_pool)
        greenlet.join()
        self._cluster_row = greenlet.get()

        authenticator = \
            InteractionPoolAuthenticator(memcached_client, 
                                         self._interaction_pool)

        self._zeromq_context = zmq.Context()

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

        self._redis_sink = OperationalStatsRedisSink(halt_event, 
                                                     redis_queue,
                                                     _local_node_name)
        self._redis_sink.link_exception(self._unhandled_greenlet_exception)

        self.application = Application(
            self._interaction_pool,
            self._cluster_row,
            self._id_translator,
            authenticator,
            self._accounting_client,
            self._event_push_client,
            redis_queue
        )
        self.wsgi_server = WSGIServer(
            (_web_public_reader_host, _web_public_reader_port), 
            application=self.application,
            backlog=_wsgi_backlog,
            log=sys.stdout
        )

    def start(self):
        self._space_accounting_dealer_client.start()
        self._redis_sink.start()
        self.wsgi_server.start()

    def stop(self):
        self._log.info("stopping wsgi web server")
        self.wsgi_server.stop()
        self._accounting_client.close()
        self._log.debug("killing greenlets")
        self._space_accounting_dealer_client.kill()
        self._log.debug("joining greenlets")
        self._space_accounting_dealer_client.join()
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
        web_public_reader = WebPublicReaderServer(halt_event)
        web_public_reader.start()
    except Exception, instance:
        log.exception(str(instance))
        return -1

    halt_event.wait()
    log.info("halt_event set")

    try:
        web_public_reader.stop()
    except Exception, instance:
        log.exception(str(instance))
        return -1

    log.info("program terminates normally")
    return 0

if __name__ == '__main__':
    sys.exit(main(*sys.argv[1:]))
