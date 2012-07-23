# -*- coding: utf-8 -*-
"""
ping_view.py

A View to respond to a ping, presumably from the services availability 
monitor.
"""
import logging

import flask

from web_manager.connection_pool_view import ConnectionPoolView

rules = ["/ping", ]
endpoint = "ping"

class PingView(ConnectionPoolView):
    methods = ["GET", ]

    def dispatch_request(self):
        log = logging.getLogger("PingView")
        log.debug("responding to ping")

        return flask.Response(status=200)

view_function = PingView.as_view(endpoint)


