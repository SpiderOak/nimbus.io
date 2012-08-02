# -*- coding: utf-8 -*-
"""
web_collection_manager_main.py

web collection manager
"""
import logging
import os

import psycopg2
import psycopg2.extensions
from psycopg2.pool import ThreadedConnectionPool

from tools.standard_logging import initialize_logging
from tools.database_connection import central_database_name, \
    central_database_user

from web_collection_manager.connection_pool_view import ConnectionPoolView
from web_collection_manager import ping_view
from web_collection_manager import list_collections_view
from web_collection_manager import create_collection_view
from web_collection_manager import delete_collection_view
from web_collection_manager import set_collection_attribute_view

_local_node_name = os.environ["NIMBUSIO_NODE_NAME"]
_log_path = "{0}/nimbusio_web_collection_manager_{1}.log".format(
    os.environ["NIMBUSIO_LOG_DIR"], _local_node_name)
_management_host = os.environ['NIMBUSIO_WEB_COLLECTION_MANAGER_HOST']
_management_port = int(os.environ['NIMBUSIO_WEB_COLLECTION_MANAGER_PORT'])

_min_database_pool_connections = 1
_max_database_pool_connections = 5
_database_credentials = {
    "database"  : central_database_name,
    "user"      : central_database_user,
    "password"  : os.environ["NIMBUSIO_CENTRAL_USER_PASSWORD"],
    "host"      : os.environ["NIMBUSIO_CENTRAL_DATABASE_HOST"],
    "port"      : os.environ["NIMBUSIO_CENTRAL_DATABASE_PORT"],
}

_views = [ping_view,
          list_collections_view, 
          create_collection_view,
          delete_collection_view, 
          set_collection_attribute_view]

from flask import Flask
app = Flask("web_collection_manager")

if not app.debug:
    initialize_logging(_log_path)

app.logger.info("creating connection pool")
ConnectionPoolView.connection_pool = \
    ThreadedConnectionPool(_min_database_pool_connections,
                           _max_database_pool_connections,
                           **_database_credentials)

for view in _views:
    app.logger.info("loading {0}".format(view.endpoint))
    for rule in view.rules:
        app.add_url_rule(rule, 
                         endpoint=view.endpoint, 
                         view_func=view.view_function)

if __name__ == "__main__":
    log = logging.getLogger("__main__")
    log.info("program starts")

    log.info("app.run(host={0}, port={1})".format(_management_host, 
        str(_management_port)))
    app.run(host=_management_host, port=_management_port)
    

