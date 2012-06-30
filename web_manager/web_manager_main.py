# -*- coding: utf-8 -*-
"""
web_manager.py

web manager
"""
import logging
import os

import psycopg2
import psycopg2.extensions
from psycopg2.pool import ThreadedConnectionPool

from tools.standard_logging import initialize_logging
from tools.database_connection import central_database_name, \
    central_database_user

from web_manager.connection_pool_view import ConnectionPoolView
from web_manager import list_collections_view

_local_node_name = os.environ["NIMBUSIO_NODE_NAME"]
_log_path = "{0}/nimbusio_web_manager_{1}.log".format(
    os.environ["NIMBUSIO_LOG_DIR"], _local_node_name)
_management_api_dest = os.environ["NIMBUSIO_MANAGEMENT_API_REQUEST_DEST"]

_min_database_pool_connections = 1
_max_database_pool_connections = 5
_database_credentials = {
    "database"  : central_database_name,
    "user"      : central_database_user,
    "password"  : os.environ["NIMBUSIO_CENTRAL_USER_PASSWORD"],
    "host"      : os.environ["NIMBUSIO_CENTRAL_DATABASE_HOST"],
    "port"      : os.environ["NIMBUSIO_CENTRAL_DATABASE_PORT"],
}

_views = [list_collections_view, ]

from flask import Flask
app = Flask("web_manager")

if __name__ == "__main__":
    initialize_logging(_log_path)
    log = logging.getLogger("__main__")
    log.info("program starts")

    host, port_str = _management_api_dest.split(":")

    log.info("creating connection pool")
    ConnectionPoolView.connection_pool = \
        ThreadedConnectionPool(_min_database_pool_connections,
                               _max_database_pool_connections,
                               **_database_credentials)

    log.info("loading views")
    try:
        for view in _views:
            log.info("loading {0}".format(view.endpoint))
            for rule in view.rules:
                app.add_url_rule(rule, 
                                 endpoint=view.endpoint, 
                                 view_func=view.view_function)
    except Exception:
        log.exception("loading views")

    try:
        app.run(host=host, port=int(port_str))
    except Exception:
        log.exception("app.run")
    
    log.info("closing connection pool")
    ConnectionPoolView.connection_pool.closeall()

    log.info("program terminates")
