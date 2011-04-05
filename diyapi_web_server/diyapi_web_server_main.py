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
from diyapi_tools.greenlet_push_client import GreenletPUSHClient

from diyapi_web_server.application import Application
from diyapi_web_server.data_writer import DataWriter
from diyapi_web_server.data_reader import DataReader
from diyapi_web_server.database_client import DatabaseClient
from diyapi_web_server.space_accounting_client import SpaceAccountingClient
from diyapi_web_server.sql_authenticator import SqlAuthenticator


_log_path = "/var/log/pandora/diyapi_web_server.log"

DB_HOST = os.environ['PANDORA_DATABASE_HOST']
DB_NAME = 'pandora'
DB_USER = 'diyapi'

NODE_NAMES = os.environ['SPIDEROAK_MULTI_NODE_NAME_SEQ'].split()
LOCAL_NODE_NAME = os.environ["SPIDEROAK_MULTI_NODE_NAME"]
DATA_READER_ADDRESSES = \
    os.environ["DIYAPI_DATA_READER_ADDRESSES"].split()
DATA_WRITER_ADDRESSES = \
    os.environ["DIYAPI_DATA_WRITER_ADDRESSES"].split()
DATABASE_SERVER_ADDRESSES = \
    os.environ["DIYAPI_DATABASE_SERVER_ADDRESSES"].split()
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
            host=DB_HOST
        )
        authenticator = SqlAuthenticator(db_connection)

        self._zeromq_context = zmq.context.Context()

        self._pollster = GreenletZeroMQPollster()

        self._data_writers = list()
        for node_name, data_writer_address in zip(
            NODE_NAMES, DATA_WRITER_ADDRESSES
        ):
            xreq_client = GreenletXREQClient(
                self._zeromq_context, 
                node_name, 
                data_writer_address
            )
            xreq_client.register(self._pollster)
            data_writer = DataWriter(
                node_name, xreq_client
            )
            self._data_writers.append(data_writer)

        self._data_readers = list()
        for node_name, data_reader_address in zip(
            NODE_NAMES, DATA_READER_ADDRESSES
        ):
            xreq_client = GreenletXREQClient(
                self._zeromq_context, 
                node_name, 
                data_reader_address
            )
            xreq_client.register(self._pollster)
            data_reader = DataReader(
                node_name, xreq_client
            )
            self._data_readers.append(data_reader)

        self._database_clients = list()
        for node_name, database_server_address in zip(
            NODE_NAMES, DATABASE_SERVER_ADDRESSES
        ):
            xreq_client = GreenletXREQClient(
                self._zeromq_context, 
                node_name, 
                database_server_address
            )
            xreq_client.register(self._pollster)
            database_client = DatabaseClient(
                node_name, xreq_client
            )
            self._database_clients.append(database_client)

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
        push_client.register(self._pollster)

        self._accounting_client = SpaceAccountingClient(
            LOCAL_NODE_NAME,
            xreq_client,
            push_client
        )

        self.application = Application(
            self._data_writers,
            self._data_readers,
            self._database_clients,
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
        for data_writer in self._data_writers:
            data_writer.close()
        for data_reader in self._data_readers:
            data_reader.close()
        for database_client in self._database_clients:
            database_client.close()
        self._pollster.kill()
        self._zeromq_context.term()

    def serve_forever(self):
        self.start()
        self._stopped_event.wait()


def main():
    initialize_logging(_log_path)
    WebServer().serve_forever()
    return 0


if __name__ == '__main__':
    sys.exit(main(*sys.argv[1:]))
