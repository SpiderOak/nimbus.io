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
patch_all(httplib=True)

import gevent_zeromq
gevent_zeromq.monkey_patch()

import logging
import os
import os.path
import pickle
import signal
import sys

from gevent.pywsgi import WSGIServer
from gevent.event import Event
from gevent_zeromq import zmq
import gevent

from tools.standard_logging import initialize_logging
from tools.greenlet_dealer_client import GreenletDealerClient
from tools.greenlet_push_client import GreenletPUSHClient
from tools.database_connection import get_central_connection, \
        get_node_local_connection
from tools.event_push_client import EventPushClient
from tools.id_translator import InternalIDTranslator

from web_server.application import Application
from web_server.space_accounting_client import SpaceAccountingClient
from web_server.sql_authenticator import SqlAuthenticator
from web_server.central_database_util import get_cluster_row

_log_path = "%s/nimbusio_web_server_%s.log" % (
    os.environ["NIMBUSIO_LOG_DIR"], os.environ["NIMBUSIO_NODE_NAME"], )

_local_node_name = os.environ["NIMBUSIO_NODE_NAME"]
_space_accounting_server_address = \
    os.environ["NIMBUSIO_SPACE_ACCOUNTING_SERVER_ADDRESS"]
_space_accounting_pipeline_address = \
    os.environ["NIMBUSIO_SPACE_ACCOUNTING_PIPELINE_ADDRESS"]
_web_server_host = os.environ.get("NIMBUSIO_WEB_SERVER_HOST", "")
_web_server_port = int(os.environ.get("NIMBUSIO_WEB_SERVER_PORT", "8088"))
_wsgi_backlog = int(os.environ.get("NIMBUS_IO_WSGI_BACKLOG", "1024"))
_repository_path = os.environ["NIMBUSIO_REPOSITORY_PATH"]

def _signal_handler_closure(halt_event):
    def _signal_handler(*_args):
        halt_event.set()
    return _signal_handler

class WebServer(object):
    def __init__(self):
        self._log = logging.getLogger("WebServer")
        authenticator = SqlAuthenticator()

        self._central_connection = get_central_connection()
        self._cluster_row = get_cluster_row(self._central_connection)
        self._node_local_connection = get_node_local_connection()

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
        self.application = Application(
            self._central_connection,
            self._node_local_connection,
            self._cluster_row,
            self._id_translator,
            authenticator,
            self._accounting_client,
            self._event_push_client
        )
        self.wsgi_server = WSGIServer(
            (_web_server_host, _web_server_port), 
            application=self.application,
            backlog=_wsgi_backlog
        )

    def start(self):
        self._space_accounting_dealer_client.start()
        self.wsgi_server.start()

    def stop(self):
        self._log.info("stopping wsgi web server")
        self.wsgi_server.stop()
        self._accounting_client.close()
        self._log.debug("killing greenlets")
        self._space_accounting_dealer_client.kill()
        self._log.debug("joining greenlets")
        self._space_accounting_dealer_client.join()
        self._log.debug("closing zmq")
        self._event_push_client.close()
        self._zeromq_context.term()
        self._log.info("closing database connections")
        self._central_connection.close()
        self._node_local_connection.close()

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
        web_server = WebServer()
        web_server.start()
    except Exception, instance:
        log.exception(str(instance))
        return -1

    halt_event.wait()
    log.info("halt_event set")

    try:
        web_server.stop()
    except Exception, instance:
        log.exception(str(instance))
        return -1

    log.info("program terminates normally")
    return 0

if __name__ == '__main__':
    sys.exit(main(*sys.argv[1:]))
