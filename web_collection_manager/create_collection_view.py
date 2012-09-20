# -*- coding: utf-8 -*-
"""
create_collection_view.py

A View to create a collection for a user
"""
import httplib
import json
import logging

import flask

from tools.greenlet_database_util import GetConnection
from tools.collection import valid_collection_name
from tools.data_definitions import http_timestamp_str
from tools.customer_key_lookup import CustomerKeyConnectionLookup
from tools.collection_access_control import cleanse_access_control

from web_collection_manager.connection_pool_view import ConnectionPoolView
from web_collection_manager.authenticator import authenticate

class CreateCollectionError(Exception):
    pass

rules = ["/customers/<username>/collections", ]
endpoint = "create_collection"

def _create_collection(cursor, 
                       username, 
                       collection_name, 
                       versioning, 
                       access_control):
    """
    create a collection for the customer
    """
    assert valid_collection_name(collection_name)

    # XXX: for now just select a cluster at random to assign the collection to.
    # the real management API code needs more sophisticated cluster selection.
    cursor.execute("""
        insert into nimbusio_central.collection
        (name, customer_id, cluster_id, versioning, access_control)
        values (%s, 
                (select id from nimbusio_central.customer where username = %s),
                (select id from nimbusio_central.cluster 
                 order by random() limit 1),
                %s)
        returning creation_time
    """, [collection_name, username, versioning, access_control])
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

        # Ticket # 43 Implement access_control properties for collections
        if flask.request.headers['Content-Type'] == 'application/json':
            access_control, error_list = \
                cleanse_access_control(flask.request.data)
            if error_list is not None:
                result_dict = {"success"    : False,
                               "error_list" : error_list, }
                return flask.Response(json.dumps(result_dict, 
                                                 sort_keys=True, 
                                                 indent=4), 
                                      status=httplib.BAD_REQUEST,
                                      content_type="application/json")
        else:
            access_control = None

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
            cursor.execute("begin")
            try:
                creation_time = _create_collection(cursor, 
                                                   username, 
                                                   collection_name, 
                                                   versioning,
                                                   access_control)
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
            "creation-time" : http_timestamp_str(creation_time)} 

        # 2012-04-15 dougfort Ticket #12 - return 201 'created'
        # 2012-08-16 dougfort Ticket #28 - set content_type
        # 2012-08-16 dougfort Ticket #29 - format json for debuging
        return flask.Response(json.dumps(collection_dict, 
                                         sort_keys=True, 
                                         indent=4), 
                              status=httplib.CREATED,
                              content_type="application/json")

view_function = CreateCollectionView.as_view(endpoint)

