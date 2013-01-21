# -*- coding: utf-8 -*-
"""
set_collection_attribute_view.py

A View to to set an attribute of a collection for a user
"""
import httplib
import json
import logging

import flask

from tools.greenlet_database_util import GetConnection
from tools.customer_key_lookup import CustomerKeyConnectionLookup
from tools.collection_access_control import cleanse_access_control

from web_collection_manager.connection_pool_view import ConnectionPoolView
from web_collection_manager.authenticator import authenticate

rules = ["/customers/<username>/collections/<collection_name>", ]
endpoint = "set_collection_attribute"

def _set_collection_versioning(cursor, 
                               user_request_id, 
                               customer_id, 
                               collection_name, 
                               value):
    """
    set the versioning attribute of the collection
    """
    log = logging.getLogger("_set_collection_versioning")
    if value.lower() == "true":
        versioning = True
    elif value.lower() == "false":
        versioning = False
    else:
        error_message = "Invalid versioning value '{0}'".format(value)
        log.error("user_request_id = {0}, {1}".format(user_request_id,
                                                      error_message))
        collection_dict = {"success" : False,
                           "error_message" : error_message}
        return httplib.BAD_REQUEST, collection_dict

    cursor.execute("""update nimbusio_central.collection
                   set versioning = %s
                   where customer_id = %s and name = %s""", 
                   [versioning, customer_id, collection_name, ])

    # Ticket #49 collection manager allows authenticated users to set 
    # versioning property on collections they don't own
    if cursor.rowcount == 0:
        log.error("user_request_id = {0}, " \
                  "attempt to set version on unknown "\
                  "collection {1} {2}".format(user_request_id,
                                              customer_id, 
                                              collection_name))
        collection_dict = {"success" : False}
        return httplib.FORBIDDEN, collection_dict

    log.info("user_request_id = {0}, " \
             "versioning set to {1}".format(user_request_id, versioning))
    collection_dict = {"success" : True}
    return httplib.OK, collection_dict

def _set_collection_access_control(cursor, 
                                   user_request_id,
                                   customer_id, 
                                   collection_name, 
                                   _value):
    """
    set the access_control attribute of the collection
    """
    log = logging.getLogger("_set_collection_access_control")

    # Ticket # 43 Implement access_control properties for collections
    access_control = None
    if "Content-Type" in flask.request.headers and \
        flask.request.headers['Content-Type'] == 'application/json':
        access_control, error_list = \
            cleanse_access_control(flask.request.data)
        if error_list is not None:
            log.error("user_request_id = {0}, {1}".format(user_request_id, 
                                                          error_list))
            result_dict = {"success"    : False,
                           "error_list" : error_list, }
            return httplib.BAD_REQUEST, result_dict

    cursor.execute("""update nimbusio_central.collection
                   set access_control = %s
                   where customer_id = %s and name = %s""", 
                   [access_control, customer_id, collection_name, ])

    # Ticket #49 collection manager allows authenticated users to set 
    # versioning property on collections they don't own
    if cursor.rowcount == 0:
        log.error("user_request_id = {0}, " \
                  "unknown collection {1} {2}".format(user_request_id,
                                                      customer_id, 
                                                      collection_name))
        collection_dict = {"success" : False}
        return httplib.FORBIDDEN, collection_dict

    log.info("user_request_id = {0}, " \
             "access_control set to {1}".format(user_request_id, 
                                                access_control))

    collection_dict = {"success" : True}
    return httplib.OK, collection_dict

_dispatch_table = {"versioning"        : _set_collection_versioning,
                   "access_control"    : _set_collection_access_control }

class SetCollectionAttributeView(ConnectionPoolView):
    methods = ["PUT", ]

    def dispatch_request(self, username, collection_name):
        log = logging.getLogger("SetCollectionAttributeView")
        user_request_id = flask.request.headers["x-nimbus-io-user-request-id"]
        log.info("user_request_id = {0}, " \
                 "user_name = {1}, " \
                 "collection_name = {2}".format(user_request_id,
                                                username, 
                                                collection_name))

        with GetConnection(self.connection_pool) as connection:

            customer_key_lookup = \
                CustomerKeyConnectionLookup(self.memcached_client,
                                            connection)
            customer_id = authenticate(customer_key_lookup,
                                       username,
                                       flask.request)
            if customer_id is None:
                log.info("user_request_id = {0}, " \
                         "unauthorized".format(user_request_id))
                flask.abort(httplib.UNAUTHORIZED)

            cursor = connection.cursor()
            attribute = None
            arg_value = None
            status = None
            result_dict = None
            for key in flask.request.args:
                if key not in _dispatch_table:
                    error_message = "unknown attribute '{0}'".format(
                        flask.request.args)
                    log.error("user_request_id = {0}, " \
                              "{1}".format(user_request_id, 
                                           error_message))
                    result_dict = {"success" : False,
                                   "error_message" : error_message}
                    status = httplib.METHOD_NOT_ALLOWED
                    break
                attribute = key
                arg_value = flask.request.args[attribute]
                break

            if attribute is not None:
                try:
                    status, result_dict = \
                        _dispatch_table[attribute](cursor,
                                                   user_request_id, 
                                                   customer_id,
                                                   collection_name, 
                                                   arg_value)
                except Exception:
                    log.exception("user_request_id = {0}, " \
                                  "{1} {2}".format(user_request_id, 
                                                   collection_name, 
                                                   attribute))
                    cursor.close()
                    connection.rollback()
                    raise

            cursor.close()
            connection.commit()

        # Ticket #33 Make Nimbus.io API responses consistently JSON
        data = json.dumps(result_dict, sort_keys=True, indent=4) 

        response = flask.Response(data, 
                                  status=status,
                                  content_type="application/json")
        response.headers["content-length"] = str(len(data))
        return response

view_function = SetCollectionAttributeView.as_view(endpoint)

