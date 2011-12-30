#d -*- coding: utf-8 -*-
"""
Receives HTTP requests and distributes data to backend processes using zeromq

The web server uses gevent instead of the time queue event loop, so it has
some special modules to use gevent.

The web server has a GreenletResilientClient for each data writer and for each
data reader.

The reslient clients use Deliverator to deliver their messages.
"""
from gevent import monkey
# you must use the latest gevent and have c-ares installed for this to work
# with /etc/hosts 
# hg clone https://bitbucket.org/denis/gevent
monkey.patch_all()

import gevent_zeromq
gevent_zeromq.monkey_patch()

import logging
import os
import signal
import sys

from gevent.pywsgi import WSGIServer
from gevent.event import Event
import gevent.pool
from gevent_zeromq import zmq

from tools.standard_logging import initialize_logging
from tools.greenlet_dealer_client import GreenletDealerClient
from tools.greenlet_resilient_client import GreenletResilientClient
from tools.greenlet_pull_server import GreenletPULLServer
from tools.deliverator import Deliverator
from tools.greenlet_push_client import GreenletPUSHClient
from tools.database_connection import get_central_connection, \
        get_node_local_connection
from tools.event_push_client import EventPushClient

from web_server.application import Application
from web_server.data_reader import DataReader
from web_server.space_accounting_client import SpaceAccountingClient
from web_server.sql_authenticator import SqlAuthenticator
from web_server.watcher import Watcher

_log_path = "%s/nimbusio_web_server_%s.log" % (
    os.environ["NIMBUSIO_LOG_DIR"], os.environ["NIMBUSIO_NODE_NAME"], )

_node_names = os.environ['NIMBUSIO_NODE_NAME_SEQ'].split()
_local_node_name = os.environ["NIMBUSIO_NODE_NAME"]
_client_tag = "web-server-%s" % (_local_node_name, )
_web_server_pipeline_address = \
    os.environ["NIMBUSIO_WEB_SERVER_PIPELINE_ADDRESS"]
_data_reader_addresses = \
    os.environ["NIMBUSIO_DATA_READER_ADDRESSES"].split()
_data_writer_addresses = \
    os.environ["NIMBUSIO_DATA_WRITER_ADDRESSES"].split()
_space_accounting_server_address = \
    os.environ["NIMBUSIO_SPACE_ACCOUNTING_SERVER_ADDRESS"]
_space_accounting_pipeline_address = \
    os.environ["NIMBUSIO_SPACE_ACCOUNTING_PIPELINE_ADDRESS"]
_web_server_host = os.environ.get("NIMBUSIO_WEB_SERVER_HOST", "")
_web_server_port = int(os.environ.get("NIMBUSIO_WEB_SERVER_PORT", "8088"))
_wsgi_backlog = int(os.environ.get("NIMBUS_IO_WSGI_BACKLOG", "1024"))
_stats = {
    "archives"    : 0,
    "retrieves"   : 0,
}

def _signal_handler_closure(halt_event):
    def _signal_handler(*args):
        halt_event.set()
    return _signal_handler

class WebServer(object):
    def __init__(self):
        self._log = logging.getLogger("WebServer")
        authenticator = SqlAuthenticator()

        self._central_connection = get_central_connection()
        self._node_local_connection = get_node_local_connection()

        self._greenlet_group = gevent.pool.Group()

        self._deliverator = Deliverator()

        self._zeromq_context = zmq.Context()

        self._pull_server = GreenletPULLServer(
            self._zeromq_context, 
            _web_server_pipeline_address,
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
                _web_server_pipeline_address,
                self._deliverator
            )
            resilient_client.link_exception(self._unhandled_greenlet_exception)
            self._data_writer_clients.append(resilient_client)

        self._data_reader_clients = list()
        self._data_readers = list()
        for node_name, address in zip(_node_names, _data_reader_addresses):
            resilient_client = GreenletResilientClient(
                self._zeromq_context, 
                node_name,
                address,
                _client_tag,
                _web_server_pipeline_address,
                self._deliverator
            )
            resilient_client.link_exception(self._unhandled_greenlet_exception)
            self._data_reader_clients.append(resilient_client)
            data_reader = DataReader(
                node_name, resilient_client
            )
            self._data_readers.append(data_reader)

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

        self._watcher = Watcher(
            _stats, 
            self._data_reader_clients,
            self._data_writer_clients,
            self._event_push_client
        )

        self.application = Application(
            self._central_connection,
            self._node_local_connection,
            self._data_writer_clients,
            self._data_readers,
            authenticator,
            self._accounting_client,
            self._event_push_client,
            _stats
        )
        self.wsgi_server = WSGIServer(
            (_web_server_host, _web_server_port), 
            application=self.application,
            backlog=_wsgi_backlog
        )

    def start(self):
        self._greenlet_group.start(self._space_accounting_dealer_client)
        self._greenlet_group.start(self._pull_server)
        self._greenlet_group.start(self._watcher)
        for client in self._data_writer_clients:
            self._greenlet_group.start(client)
        for client in self._data_reader_clients:
            self._greenlet_group.start(client)
        self.wsgi_server.start()

    def stop(self):
        self.wsgi_server.stop()
        self._accounting_client.close()
        self._greenlet_group.kill()
        self._greenlet_group.join()
        self._event_push_client.close()
        self._zeromq_context.term()
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

    web_server = WebServer()
    try:
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

    return 0

if __name__ == '__main__':
    sys.exit(main(*sys.argv[1:]))
