# -*- coding: utf-8 -*-
"""
list_collections_view.py

A View to list collections for a user
"""
import httplib
import json
import logging

import flask

from tools.greenlet_database_util import GetConnection
from tools.data_definitions import http_timestamp_str
from tools.customer_key_lookup import CustomerKeyConnectionLookup

from web_collection_manager.connection_pool_view import ConnectionPoolView
from web_collection_manager.authenticator import authenticate

rules = ["/customers/<username>/collections", ]
endpoint = "list_collections"

def _list_collections(connection, customer_id):
    """
    list all collections for the customer, for all clusters
    """
    cursor = connection.cursor()
    cursor.execute("""
        select name, versioning, creation_time from nimbusio_central.collection   
        where customer_id = %s and deletion_time is null
        """, [customer_id, ])
    result = cursor.fetchall()
    cursor.close()

    return result

class ListCollectionsView(ConnectionPoolView):
    methods = ["GET", ]

    def dispatch_request(self, username):
        log = logging.getLogger("ListCollectionsView")
        log.info("user_name = {0}".format(username))

        with GetConnection(self.connection_pool) as connection:

            customer_key_lookup = \
                CustomerKeyConnectionLookup(self.memcached_client,
                                            connection)
            customer_id = authenticate(customer_key_lookup,
                                       username,
                                       flask.request)
            if customer_id is None:
                flask.abort(httplib.UNAUTHORIZED)

            collection_list = _list_collections(connection, customer_id)

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
                              status=httplib.OK,
                              content_type="application/json")

view_function = ListCollectionsView.as_view(endpoint)

