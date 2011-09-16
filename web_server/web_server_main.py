# -*- coding: utf-8 -*-
"""
web_server_main.py

Receives HTTP requests and distributes data to backend processes over amqp.
"""
import gevent
from gevent import monkey
monkey.patch_all(dns=False)

import os
import sys

from gevent.pywsgi import WSGIServer
from gevent.event import Event
import zmq

from tools.standard_logging import initialize_logging
from tools.greenlet_zeromq_pollster import GreenletZeroMQPollster
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

_log_path = "%s/nimbusio_web_server.log" % (os.environ["NIMBUSIO_LOG_DIR"], )

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

class WebServer(object):
    def __init__(self):
        authenticator = SqlAuthenticator()

        self._central_connection = get_central_connection()
        self._node_local_connection = get_node_local_connection()

        self._deliverator = Deliverator()

        self._zeromq_context = zmq.Context()

        self._pollster = GreenletZeroMQPollster()

        self._pull_server = GreenletPULLServer(
            self._zeromq_context, 
            _web_server_pipeline_address,
            self._deliverator
        )
        self._pull_server.register(self._pollster)

        self._data_writer_clients = list()
        for node_name, address in zip(_node_names, _data_writer_addresses):
            resilient_client = GreenletResilientClient(
                self._zeromq_context, 
                self._pollster,
                node_name,
                address,
                _client_tag,
                _web_server_pipeline_address,
                self._deliverator
            )
            self._data_writer_clients.append(resilient_client)

        self._data_readers = list()
        for node_name, address in zip(_node_names, _data_reader_addresses):
            resilient_client = GreenletResilientClient(
                self._zeromq_context, 
                self._pollster,
                node_name,
                address,
                _client_tag,
                _web_server_pipeline_address,
                self._deliverator
            )
            data_reader = DataReader(
                node_name, resilient_client
            )
            self._data_readers.append(data_reader)

        dealer_client = GreenletDealerClient(
            self._zeromq_context, 
            _local_node_name, 
            _space_accounting_server_address
        )
        dealer_client.register(self._pollster)

        push_client = GreenletPUSHClient(
            self._zeromq_context, 
            _local_node_name, 
            _space_accounting_pipeline_address,
        )

        self._accounting_client = SpaceAccountingClient(
            _local_node_name,
            dealer_client,
            push_client
        )

        self._event_push_client = EventPushClient(
            self._zeromq_context,
            "web-server"
        )

        self.application = Application(
            self._central_connection,
            self._node_local_connection,
            self._data_writer_clients,
            self._data_readers,
            authenticator,
            self._accounting_client,
            self._event_push_client
        )
        self.wsgi_server = WSGIServer(
            (_web_server_host, _web_server_port), 
            self.application
        )
        self._stopped_event = Event()

    def start(self):
        self._stopped_event.clear()
        self._pollster.start()
        self.wsgi_server.start()

    def stop(self):
        self.wsgi_server.stop()
        self._stopped_event.set()
        self._accounting_client.close()
        for client in self._data_writer_clients:
            client.close()
        for data_reader in self._data_readers:
            data_reader.close()
        self._pull_server.close()
        self._pollster.kill()
        self._pollster.join(timeout=3.0)
        self._event_push_client.close()
        self._zeromq_context.term()
        self._central_connection.close()
        self._node_local_connection.close()

    def serve_forever(self):
        self.start()
        self._stopped_event.wait()


def main():
    initialize_logging(_log_path)
    WebServer().serve_forever()
    return 0


if __name__ == '__main__':
    sys.exit(main(*sys.argv[1:]))
