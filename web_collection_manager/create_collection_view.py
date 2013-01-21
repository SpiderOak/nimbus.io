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
class InvalidCollectionName(CreateCollectionError):
    pass
class DuplicateCollection(CreateCollectionError):
    pass

rules = ["/customers/<username>/collections", ]
endpoint = "create_collection"

def _create_collection(cursor, 
                       customer_id, 
                       collection_name, 
                       versioning, 
                       access_control):
    """
    create a collection for the customer
    """
    cursor.execute("""select count(id) from nimbusio_central.collection
                      where name = %s""", [collection_name, ])
    (count, ) = cursor.fetchone()
    if count != 0:
        raise DuplicateCollection(collection_name)

    # XXX: for now just select a cluster at random to assign the collection to.
    # the real management API code needs more sophisticated cluster selection.
    cursor.execute("""
        insert into nimbusio_central.collection
        (name, customer_id, cluster_id, versioning, access_control)
        values (%s, 
                %s,
                (select id from nimbusio_central.cluster 
                 order by random() limit 1),
                %s,
                %s)
        returning creation_time
    """, [collection_name, customer_id, versioning, access_control])
    (creation_time, ) = cursor.fetchone()

    return creation_time

class CreateCollectionView(ConnectionPoolView):
    methods = ["POST", ]

    def dispatch_request(self, username):
        log = logging.getLogger("CreateCollectionView")
        user_request_id = flask.request.headers["x-nimbus-io-user-request-id"]
        log.info("user_request_id = {0}, user_name = {1}, " \
                 "collection_name = {2}".format(user_request_id, 
                                                username, 
                                                flask.request.args["name"]))
        assert flask.request.args["action"] == "create", flask.request.args

        collection_name = flask.request.args["name"]
        if not valid_collection_name(collection_name):
            # Ticket #48 Creating collection incorrectly handles 
            # creating colliding collections
            log.error("user_request_id = {0}, " \
                      "invalid collection name '{1}'".format(user_request_id,
                                                             collection_name))
            collection_dict = {
                "name"           : collection_name,
                "error-messages" : ["Invalid Name"]} 
            data = json.dumps(collection_dict, sort_keys=True, indent=4) 
            response = flask.Response(data,
                                      status=httplib.CONFLICT,
                                      content_type="application/json")
            response.headers["content-length"] = str(len(data))
            return response

        versioning = False

        # Ticket # 43 Implement access_control properties for collections
        if "Content-Type" in flask.request.headers and \
            flask.request.headers['Content-Type'] == 'application/json':
            access_control, error_list = \
                cleanse_access_control(flask.request.data)
            if error_list is not None:
                log.error("user_request_id = {0}, " \
                          "invalid access control '{1}'".format(user_request_id,
                                                                error_list))
                result_dict = {"success"    : False,
                               "error_list" : error_list, }
                data = json.dumps(result_dict, sort_keys=True, indent=4) 
                response = flask.Response(data, 
                                          status=httplib.BAD_REQUEST,
                                          content_type="application/json")
                response.headers["content-length"] = str(len(data))
                return response
        else:
            access_control = None

        with GetConnection(self.connection_pool) as connection:

            customer_key_lookup = \
                CustomerKeyConnectionLookup(self.memcached_client,
                                            connection)
            customer_id = authenticate(customer_key_lookup,
                                       username,
                                       flask.request)            
            if customer_id is None:
                log.info("user_request_id = {0}, unauthorized")
                flask.abort(httplib.UNAUTHORIZED)

            cursor = connection.cursor()
            cursor.execute("begin")
            try:
                creation_time = _create_collection(cursor, 
                                                   customer_id, 
                                                   collection_name, 
                                                   versioning,
                                                   access_control)
            except DuplicateCollection:
                cursor.close()
                connection.rollback()
                # Ticket #48 Creating collection incorrectly handles 
                # creating colliding collections
                log.error("user_request_id = {0}, " \
                          "duplicate collection name '{1}'".format(
                          user_request_id, collection_name))

                collection_dict = {
                    "name"           : collection_name,
                    "error-messages" : ["Invalid Name"]} 
                data = json.dumps(collection_dict, sort_keys=True, indent=4) 
                response = flask.Response(data, 
                                          status=httplib.CONFLICT,
                                          content_type="application/json")
                response.headers["content-length"] = str(len(data))
                return response

            except Exception:
                log.exception("user_request_id = {0}".format(user_request_id))
                cursor.close()
                connection.rollback()
                raise

            else:
                cursor.close()
                connection.commit()

        log.info("user_request_id = {0}, created {1}".format(user_request_id,
                                                             collection_name))

        # this is the same format returned by list_collection
        collection_dict = {
            "name" : collection_name,
            "versioning" : versioning,
            "creation-time" : http_timestamp_str(creation_time)} 

        # 2012-08-16 dougfort Ticket #29 - format json for debuging
        data = json.dumps(collection_dict, sort_keys=True, indent=4)

        # 2012-04-15 dougfort Ticket #12 - return 201 'created'
        # 2012-08-16 dougfort Ticket #28 - set content_type
        response = flask.Response(data, 
                                  status=httplib.CREATED,
                                  content_type="application/json")
        response.headers["content-length"] = str(len(data))
        return response

view_function = CreateCollectionView.as_view(endpoint)

