# -*- coding: utf-8 -*-
"""
delete_collection_view.py

A View to delete a collection for a user
"""
import logging

import flask

from tools.greenlet_database_util import GetConnection
from tools.collection import compute_default_collection_name

from web_manager.connection_pool_view import ConnectionPoolView
from web_manager.authenticator import authenticate

rules = ["/customers/<username>/collections/<collection_name>", ]
endpoint = "delete_collection"

def _delete_collection(cursor, collection_name):
    """
    mark the collection as deleted
    """
    cursor.execute("""
        update nimbusio_central.collection
        set deletion_time = current_timestamp
        where name = %s
        """, [collection_name, ])

class DeleteCollectionView(ConnectionPoolView):
    methods = ["DELETE", ]

    def dispatch_request(self, username, collection_name):
        log = logging.getLogger("DeleteCollectionView")
        log.info("user_name = {0}, collection_name = {1}".format(
            username, collection_name))

        with GetConnection(self.connection_pool) as connection:
            authenticated = authenticate(connection,
                                         username,
                                         flask.request)
            if not authenticated:
                flask.abort(401)

            # you can't delete your default collection
            default_collection_name = compute_default_collection_name(username)
            if collection_name == default_collection_name:
                log.warn("attempt to delete default collection {0}".format(
                    default_collection_name))
                flask.abort(405)

            # TODO: can't delete a collection that contains keys

            cursor = connection.cursor()
            cursor.execute("begin")
            try:
                _delete_collection(cursor, collection_name)
            except Exception:
                cursor.close()
                connection.rollback()
                raise
            else:
                cursor.close()
                connection.commit()

        return flask.Response(status=200)

view_function = DeleteCollectionView.as_view(endpoint)

