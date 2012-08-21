# -*- coding: utf-8 -*-
"""
list_collections_view.py

A View to list collections for a user
"""
import json
import logging

import flask

from tools.greenlet_database_util import GetConnection
from tools.data_definitions import http_timestamp_str
from web_collection_manager.connection_pool_view import ConnectionPoolView
from web_collection_manager.authenticator import authenticate

rules = ["/customers/<username>/collections", ]
endpoint = "list_collections"

def _list_collections(connection, username):
    """
    list all collections for the customer, for all clusters
    """
    cursor = connection.cursor()
    cursor.execute("""
        select name, versioning, creation_time from nimbusio_central.collection   
        where customer_id = (select id from nimbusio_central.customer 
                                       where username = %s) 
        and deletion_time is null
        """, [username, ])
    result = cursor.fetchall()
    cursor.close()

    return result

class ListCollectionsView(ConnectionPoolView):
    methods = ["GET", ]

    def dispatch_request(self, username):
        log = logging.getLogger("ListCollectionsView")
        log.info("user_name = {0}".format(username))

        with GetConnection(self.connection_pool) as connection:
            authenticated = authenticate(connection,
                                         username,
                                         flask.request)
            if not authenticated:
                flask.abort(401)

            collection_list = _list_collections(connection, username)

        # json won't dump datetime
        json_collections = [
            {"name" : n, 
             "versioning" : v, 
             "creation-time" : http_timestamp_str(t)} \
            for (n, v, t) in collection_list]

        # 2012-08-16 dougfort Ticket #28 - set content_type
        # 2012-08-16 dougfort Ticket #29 - format json for debuging
        return flask.Response(json.dumps(json_collections, 
                                         sort_keys=True, 
                                         indent=4), 
                              status=200,
                              content_type="application/json")

view_function = ListCollectionsView.as_view(endpoint)

