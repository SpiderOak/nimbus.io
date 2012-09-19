# -*- coding: utf-8 -*-
"""
set_collection_attribute_view.py

A View to to set an attribute of a collection for a user
At present, the only attribute we recognize is 'versioning'
"""
import json
import logging

import flask

from tools.greenlet_database_util import GetConnection
from tools.customer_key_lookup import CustomerKeyConnectionLookup

from web_collection_manager.connection_pool_view import ConnectionPoolView
from web_collection_manager.authenticator import authenticate

rules = ["/customers/<username>/collections/<collection_name>", ]
endpoint = "set_collection_attribute"

def _set_collection_versioning(cursor, collection_name, value):
    """
    set the versioning attribute of the collection
    """
    log = logging.getLogger("_set_collection_versioning")
    if value.lower() == "true":
        versioning = True
    elif value.lower() == "false":
        versioning = False
    else:
        log.error("Invalid versioning value '{0}'".format(value))
        flask.abort(405)

    cursor.execute("""update nimbusio_central.collection
                   set versioning = %s
                   where name = %s""", [versioning, collection_name, ])

_dispatch_table = {
    "versioning" : _set_collection_versioning
}

class SetCollectionAttributeView(ConnectionPoolView):
    methods = ["PUT", ]

    def dispatch_request(self, username, collection_name):
        log = logging.getLogger("SetCollectionAttributeView")

        log.info("user_name = {0}, collection_name = {1}".format(
            username, collection_name))

        with GetConnection(self.connection_pool) as connection:

            customer_key_lookup = \
                CustomerKeyConnectionLookup(self.memcached_client,
                                            connection)
            authenticated = authenticate(customer_key_lookup,
                                         username,
                                         flask.request)
            if not authenticated:
                flask.abort(401)

            cursor = connection.cursor()
            for key in flask.request.args:
                if key not in _dispatch_table:
                    log.error("unknown attribute '{0}'".format(
                        flask.request.args))
                    flask.abort(405)

                try:
                    _dispatch_table[key](cursor, 
                                         collection_name, 
                                         flask.request.args[key])
                except Exception:
                    cursor.close()
                    connection.rollback()
                    raise

            cursor.close()
            connection.commit()

        # Ticket #33 Make Nimbus.io API responses consistently JSON
        collection_dict = {"success" : True}
        return flask.Response(json.dumps(collection_dict, 
                                         sort_keys=True, 
                                         indent=4), 
                              status=200,
                              content_type="application/json")

view_function = SetCollectionAttributeView.as_view(endpoint)

