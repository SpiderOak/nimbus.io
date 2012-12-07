# -*- coding: utf-8 -*-
"""
get_collection_attribute_view.py

A View to to get an attribute of a collection for a user
"""
import httplib
import json
import logging

import flask

from tools.greenlet_database_util import GetConnection
from tools.customer_key_lookup import CustomerKeyConnectionLookup

from web_collection_manager.connection_pool_view import ConnectionPoolView
from web_collection_manager.authenticator import authenticate

rules = ["/customers/<username>/collections/<collection_name>", ]
endpoint = "get_collection_attribute"

def _get_collection_info(cursor, customer_id, collection_name, _args):
    """
    get basic information about the collection
    Seee Ticket #51 Implement GET JSON for a collection
    """
    log = logging.getLogger("_get_collection_info")

    collection_dict = {"success" : True}
    return httplib.OK, collection_dict

def _get_collection_space_usage(cursor, customer_id, collection_name, args):
    """
    get usage information for the collection
    See Ticket #66 Include operational stats in API queries for space usage
    """
    log = logging.getLogger("_get_collection_space_usage")


    collection_dict = {"success" : True}
    return httplib.OK, collection_dict

class GetCollectionAttributeView(ConnectionPoolView):
    methods = ["GET", ]

    def dispatch_request(self, username, collection_name):
        log = logging.getLogger("GetCollectionAttributeView")

        log.info("user_name = {0}, collection_name = {1}".format(
            username, collection_name))

        with GetConnection(self.connection_pool) as connection:

            customer_key_lookup = \
                CustomerKeyConnectionLookup(self.memcached_client,
                                            connection)
            customer_id = authenticate(customer_key_lookup,
                                       username,
                                       flask.request)
            if customer_id is None:
                flask.abort(httplib.UNAUTHORIZED)

            if "space_usage" in  flask.request.args:
                handler = _get_collection_space_usage
            else:
                handler = _get_collection_info

            cursor = connection.cursor()
            try:
                status, result_dict = handler(cursor, 
                                              customer_id,
                                              collection_name, 
                                              flask.request.args)
            except Exception:
                log.exception("{0} {1}".format(collection_name, 
                                               flask.request.args))
                cursor.close()
                connection.rollback()
                raise

            cursor.close()
            connection.commit()

        # Ticket #33 Make Nimbus.io API responses consistently JSON
        return flask.Response(json.dumps(result_dict, 
                                         sort_keys=True, 
                                         indent=4), 
                              status=status,
                              content_type="application/json")

view_function = GetCollectionAttributeView.as_view(endpoint)

