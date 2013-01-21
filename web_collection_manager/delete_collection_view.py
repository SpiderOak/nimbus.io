# -*- coding: utf-8 -*-
"""
delete_collection_view.py

A View to delete a collection for a user
"""
import httplib
import json
import logging

import flask

from tools.greenlet_database_util import GetConnection
from tools.collection import compute_default_collection_name
from tools.customer_key_lookup import CustomerKeyConnectionLookup

from web_collection_manager.connection_pool_view import ConnectionPoolView
from web_collection_manager.authenticator import authenticate

rules = ["/customers/<username>/collections/<collection_name>", ]
endpoint = "delete_collection"

def _delete_collection(cursor, user_request_id, customer_id, collection_name):
    """
    mark the collection as deleted
    """
    log = logging.getLogger("_delete_collection")
    cursor.execute("""
        select id
        from nimbusio_central.collection
        where customer_id = %s and name = %s""", 
        [customer_id, collection_name, ])
    result = cursor.fetchone()
    if result is None:
        log.warn("user_request_id = {0}, " \
                 "attempt to delete unknown ", \
                 "collection {1}".format(user_request_id,
                                         collection_name))
        return False
    (row_id, ) = result

    deleted_name = "".join(["__deleted__{0}__".format(row_id), 
                            collection_name, ])
    log.debug("user_request_id = {0}, " \
              "renaming deleted collection to {1}".format(user_request_id,
                                                          deleted_name))
    cursor.execute("""
        update nimbusio_central.collection
        set deletion_time = current_timestamp,
            name = %s
        where id = %s
        """, [deleted_name, row_id])

    return True

class DeleteCollectionView(ConnectionPoolView):
    methods = ["DELETE", "POST", ]

    def dispatch_request(self, username, collection_name):
        log = logging.getLogger("DeleteCollectionView")
        user_request_id = flask.request.headers["x-nimbus-io-user-request-id"]
        log.info("user_request_id = {0}, " \
                 "user_name = {1}, " \
                 "collection_name = {2}".format(user_request_id,
                                                username, 
                                                collection_name))

        assert flask.request.method == "DELETE" or \
            (flask.request.method == "POST" \
             and flask.request.args["action"] == "delete"), \
                (user_request_id, flask.request.method, flask.request.args, )

        with GetConnection(self.connection_pool) as connection:

            customer_key_lookup = \
                CustomerKeyConnectionLookup(self.memcached_client,
                                            connection)
            customer_id = authenticate(customer_key_lookup,
                                       username,
                                       flask.request)
            if customer_id is None:
                log.info("user_request_id = {0}, " \
                         "unauthroized".format(user_request_id))
                flask.abort(httplib.UNAUTHORIZED)

            # you can't delete your default collection
            default_collection_name = compute_default_collection_name(username)
            if collection_name == default_collection_name:
                log.warn("user_request_id = {0}, " \
                         "attempt to delete default collection {1}".format(
                         user_request_id, default_collection_name))
                flask.abort(httplib.METHOD_NOT_ALLOWED)

            # TODO: can't delete a collection that contains keys

            cursor = connection.cursor()
            try:
                deleted = _delete_collection(cursor, 
                                             user_request_id,
                                             customer_id, 
                                             collection_name)
            except Exception:
                log.exception("user_request_id = {0}".format(user_request_id))
                cursor.close()
                connection.rollback()
                raise

            cursor.close()

            # Ticket #39 collection manager allows authenticated users to set 
            # versioning property on collections they don't own
            if not deleted:
                log.error("user_request_id = {0}, " \
                          "forbidden".format(user_request_id))
                connection.rollback()
                flask.abort(httplib.FORBIDDEN)

            connection.commit()

        log.info("user_request_id = {0}, "\
                 "collection {1} deleted".format(user_request_id,
                                                 collection_name))

        # Ticket #33 Make Nimbus.io API responses consistently JSON
        collection_dict = {"success" : True}
        data = json.dumps(collection_dict, sort_keys=True, indent=4) 

        response = flask.Response(data, 
                                  status=httplib.OK,
                                  content_type="application/json")
        response.headers["content-length"] = str(len(data))
        return response

view_function = DeleteCollectionView.as_view(endpoint)

