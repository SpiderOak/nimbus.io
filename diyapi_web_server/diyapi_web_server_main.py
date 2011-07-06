# -*- coding: utf-8 -*-
"""
diyapi_web_server_main.py

Receives HTTP requests and distributes data to backend processes over amqp.
"""
import gevent
from gevent import monkey
monkey.patch_all(dns=False)

import os
import sys

from gevent.pywsgi import WSGIServer
from gevent.event import Event
from gevent_zeromq import zmq

import psycopg2

from diyapi_tools.standard_logging import initialize_logging
from diyapi_tools.greenlet_zeromq_pollster import GreenletZeroMQPollster
from diyapi_tools.greenlet_xreq_client import GreenletXREQClient
from diyapi_tools.greenlet_resilient_client import GreenletResilientClient
from diyapi_tools.greenlet_pull_server import GreenletPULLServer
from diyapi_tools.deliverator import Deliverator
from diyapi_tools.greenlet_push_client import GreenletPUSHClient
from diyapi_tools.pandora_database_connection import get_node_local_connection

from diyapi_web_server.application import Application
from diyapi_web_server.data_reader import DataReader
from diyapi_web_server.space_accounting_client import SpaceAccountingClient
from diyapi_web_server.sql_authenticator import SqlAuthenticator


_log_path = "/var/log/pandora/diyapi_web_server.log"

DB_HOST = os.environ['PANDORA_DATABASE_HOST']
DB_NAME = 'pandora'
DB_USER = 'diyapi'
DB_PASS = os.environ['PANDORA_DB_PW_diyapi']

NODE_NAMES = os.environ['SPIDEROAK_MULTI_NODE_NAME_SEQ'].split()
LOCAL_NODE_NAME = os.environ["SPIDEROAK_MULTI_NODE_NAME"]
CLIENT_TAG = "web-server-%s" % (LOCAL_NODE_NAME, )
WEB_SERVER_PIPELINE_ADDRESS = \
    os.environ["DIYAPI_WEB_SERVER_PIPELINE_ADDRESS"]
DATA_READER_ADDRESSES = \
    os.environ["DIYAPI_DATA_READER_ADDRESSES"].split()
DATA_WRITER_ADDRESSES = \
    os.environ["DIYAPI_DATA_WRITER_ADDRESSES"].split()
SPACE_ACCOUNTING_SERVER_ADDRESS = \
    os.environ["DIYAPI_SPACE_ACCOUNTING_SERVER_ADDRESS"]
SPACE_ACCOUNTING_PIPELINE_ADDRESS = \
    os.environ["DIYAPI_SPACE_ACCOUNTING_PIPELINE_ADDRESS"]
MAX_DOWN_EXCHANGES = 2

class WebServer(object):
    def __init__(self):
        # TODO: keep a connection pool or something
        db_connection = psycopg2.connect(
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            host=DB_HOST
        )
        authenticator = SqlAuthenticator(db_connection)

        self._node_local_connection = get_node_local_connection()

        self._deliverator = Deliverator()

        self._zeromq_context = zmq.context.Context()

        self._pollster = GreenletZeroMQPollster()

        self._pull_server = GreenletPULLServer(
            self._zeromq_context, 
            WEB_SERVER_PIPELINE_ADDRESS,
            self._deliverator
        )
        self._pull_server.register(self._pollster)

        self._data_writer_clients = list()
        for node_name, address in zip(NODE_NAMES, DATA_WRITER_ADDRESSES):
            resilient_client = GreenletResilientClient(
                self._zeromq_context, 
                self._pollster,
                node_name,
                address,
                CLIENT_TAG,
                WEB_SERVER_PIPELINE_ADDRESS,
                self._deliverator
            )
            self._data_writer_clients.append(resilient_client)

        self._data_readers = list()
        for node_name, address in zip(NODE_NAMES, DATA_READER_ADDRESSES):
            resilient_client = GreenletResilientClient(
                self._zeromq_context, 
                self._pollster,
                node_name,
                address,
                CLIENT_TAG,
                WEB_SERVER_PIPELINE_ADDRESS,
                self._deliverator
            )
            data_reader = DataReader(
                node_name, resilient_client
            )
            self._data_readers.append(data_reader)

        xreq_client = GreenletXREQClient(
            self._zeromq_context, 
            LOCAL_NODE_NAME, 
            SPACE_ACCOUNTING_SERVER_ADDRESS
        )
        xreq_client.register(self._pollster)

        push_client = GreenletPUSHClient(
            self._zeromq_context, 
            LOCAL_NODE_NAME, 
            SPACE_ACCOUNTING_PIPELINE_ADDRESS,
        )

        self._accounting_client = SpaceAccountingClient(
            LOCAL_NODE_NAME,
            xreq_client,
            push_client
        )

        self.application = Application(
            self._node_local_connection,
            self._data_writer_clients,
            self._data_readers,
            authenticator,
            self._accounting_client
        )
        self.wsgi_server = WSGIServer(('', 8088), self.application)
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
        self._zeromq_context.term()
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
