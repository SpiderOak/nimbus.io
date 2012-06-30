# -*- coding: utf-8 -*-
"""
list_collections_view.py

A View to list collections for a user
"""
import logging

from tools.greenlet_database_util import GetConnection
from web_manager.connection_pool_view import ConnectionPoolView

rules = ["/customers/<user_name>/collections", ]
endpoint = "list_collections"

class ListCollectionsView(ConnectionPoolView):
    methods = ["GET", ]

    def dispatch_request(self, user_name):
        log = logging.getLogger("ListCollectionsView")
        log.info("user_name = {0}".format(user_name))

        with GetConnection(self.connection_pool) as connection:
            pass

        return "pork"

view_function = ListCollectionsView.as_view(endpoint)

