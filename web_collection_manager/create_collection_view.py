# -*- coding: utf-8 -*-
"""
create_collection_view.py

A View to create a collection for a user
"""
import json
import logging

import flask

from tools.greenlet_database_util import GetConnection
from tools.collection import valid_collection_name
from web_collection_manager.connection_pool_view import ConnectionPoolView
from web_collection_manager.authenticator import authenticate

class CreateCollectionError(Exception):
    pass

rules = ["/customers/<username>/collections", ]
endpoint = "create_collection"

def _create_collection(cursor, username, collection_name, versioning):
    """
    create a collection for the customer
    """
    log = logging.getLogger("_create_collection")
    assert valid_collection_name(collection_name)

    # if the collecton already exists, use it
    cursor.execute("""
        select id, creation_time, deletion_time 
        from nimbusio_central.collection
        where name = %s""", [collection_name, ])
    result = cursor.fetchone()

    if result is not None:
        (row_id, creation_time, deletion_time) = result
        if deletion_time is None:
            return creation_time
        error_message = \
            "A deleted collection (id={0}) exists with this name {1}".format(
                row_id, collection_name)
        log.error(error_message)
        raise CreateCollectionError(error_message)

    # XXX: for now just select a cluster at random to assign the collection to.
    # the real management API code needs more sophisticated cluster selection.
    cursor.execute("""
        insert into nimbusio_central.collection
        (name, customer_id, cluster_id, versioning)
        values (%s, 
                (select id from nimbusio_central.customer where username = %s),
                (select id from nimbusio_central.cluster 
                 order by random() limit 1),
                %s)
        returning creation_time
    """, [collection_name, username, versioning, ])
    (creation_time, ) = cursor.fetchone()

    return creation_time

class CreateCollectionView(ConnectionPoolView):
    methods = ["POST", ]

    def dispatch_request(self, username):
        log = logging.getLogger("CreateCollectionView")
        log.info("user_name = {0}, collection_name = {1}".format(
            username, flask.request.args["name"]))
        assert flask.request.args["action"] == "create", flask.request.args

        collection_name = flask.request.args["name"]
        versioning = False

        with GetConnection(self.connection_pool) as connection:
            authenticated = authenticate(connection,
                                         username,
                                         flask.request)
            if not authenticated:
                flask.abort(401)

            cursor = connection.cursor()
            cursor.execute("begin")
            try:
                creation_time = _create_collection(cursor, 
                                                   username, 
                                                   collection_name, 
                                                   versioning)
            except Exception:
                cursor.close()
                connection.rollback()
                raise
            else:
                cursor.close()
                connection.commit()

        # this is the same format returned by list_collection
        collection_dict = {
            "name" : collection_name,
            "versioning" : versioning,
            "creation-time" : creation_time.isoformat()} 

        # 2012-04-15 dougfort Ticket #12 - return 201 'created'
        # 2012-08-16 dougfort Ticket #28 - set content_type
        return flask.Response(json.dumps(collection_dict), 
                                         status=201,
                                         content_type="application/json")

view_function = CreateCollectionView.as_view(endpoint)

