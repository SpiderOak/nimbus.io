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

import psycopg2

from diyapi_tools.amqp_connection import space_accounting_exchange_name
from diyapi_tools.standard_logging import initialize_logging

from diyapi_web_server.application import Application
from diyapi_web_server.amqp_handler import AMQPHandler
from diyapi_web_server.amqp_exchange_manager import AMQPExchangeManager
from diyapi_web_server.amqp_data_writer import AMQPDataWriter
from diyapi_web_server.amqp_space_accounter import AMQPSpaceAccounter
from diyapi_web_server.sql_authenticator import SqlAuthenticator


_log_path = "/var/log/pandora/diyapi_web_server.log"

DB_HOST = os.environ['PANDORA_DATABASE_HOST']
DB_NAME = 'pandora'
DB_USER = 'diyapi'

EXCHANGES = os.environ['DIY_NODE_EXCHANGES'].split()
MAX_DOWN_EXCHANGES = 2


class WebServer(object):
    def __init__(self):
        self.amqp_handler = AMQPHandler()
        exchange_manager = AMQPExchangeManager(EXCHANGES)
        # TODO: keep a connection pool or something
        db_connection = psycopg2.connect(
            database=DB_NAME,
            user=DB_USER,
            host=DB_HOST
        )
        authenticator = SqlAuthenticator(db_connection)
        accounter = AMQPSpaceAccounter(
            self.amqp_handler, space_accounting_exchange_name)
        data_writers = [AMQPDataWriter(self.amqp_handler, exchange)
                        for exchange in EXCHANGES]
        self.application = Application(
            self.amqp_handler,
            data_writers,
            exchange_manager,
            authenticator,
            accounter
        )
        self.wsgi_server = WSGIServer(('', 8088), self.application)
        self._stopped_event = Event()

    def start(self):
        self._stopped_event.clear()
        self.amqp_handler.start()
        self.wsgi_server.start()

    def stop(self):
        self.wsgi_server.stop()
        self.amqp_handler.stop()
        self._stopped_event.set()

    def serve_forever(self):
        self.start()
        self._stopped_event.wait()


def main():
    initialize_logging(_log_path)
    WebServer().serve_forever()
    return 0


if __name__ == '__main__':
    sys.exit(main(*sys.argv[1:]))
