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

from diyapi_web_server.application import Application
#from diyapi_web_server.amqp_data_writer import AMQPDataWriter
#from diyapi_web_server.amqp_data_reader import AMQPDataReader
from diyapi_web_server.database_client import DatabaseClient
#from diyapi_web_server.amqp_space_accounting_server import (
#    AMQPSpaceAccountingServer)
from diyapi_web_server.sql_authenticator import SqlAuthenticator


_log_path = "/var/log/pandora/diyapi_web_server.log"

DB_HOST = os.environ['PANDORA_DATABASE_HOST']
DB_NAME = 'pandora'
DB_USER = 'diyapi'

NODE_NAMES = os.environ['SPIDEROAK_MULTI_NODE_NAME_SEQ'].split()
DATABASE_SERVER_ADDRESSES = \
    os.environ["DIYAPI_DATABASE_SERVER_ADDRESSES"].split()
MAX_DOWN_EXCHANGES = 2


class WebServer(object):
    def __init__(self):
#        self.amqp_handler = AMQPHandler()
        # TODO: keep a connection pool or something
        db_connection = psycopg2.connect(
            database=DB_NAME,
            user=DB_USER,
            host=DB_HOST
        )
        self._zeromq_context = zmq.context.Context()
        self._pollster = GreenletZeroMQPollster()
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
        authenticator = SqlAuthenticator(db_connection)
#        accounting_server = AMQPSpaceAccountingServer(
#            self.amqp_handler, space_accounting_exchange_name)
        accounting_server = None
#        data_writers = [AMQPDataWriter(self.amqp_handler, exchange)
#                        for exchange in EXCHANGES]
        data_writers = list()
#        data_readers = [AMQPDataReader(self.amqp_handler, exchange)
#                        for exchange in EXCHANGES]
        data_readers = list()
        self.application = Application(
            data_writers,
            data_readers,
            self._database_clients,
            authenticator,
            accounting_server
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
