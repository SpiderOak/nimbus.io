# -*- coding: utf-8 -*-
"""
set_collection_attribute_view.py

A View to to set an attribute of a collection for a user
At present, the only attribute we recognize is 'versioning'
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
        error_message = "Invalid versioning value '{0}'".format(value)
        log.error(error_message)
        collection_dict = {"success" : False,
                           "error_message" : error_message}
        return httplib.BAD_REQUEST, collection_dict

    cursor.execute("""update nimbusio_central.collection
                   set versioning = %s
                   where name = %s""", [versioning, collection_name, ])

    collection_dict = {"success" : True}
    return httplib.OK, collection_dict

def _set_collection_access_control(cursor, collection_name, _value):
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
            log.error("{0}".format(error_list))
            result_dict = {"success"    : False,
                           "error_list" : error_list, }
            return httplib.BAD_REQUEST, result_dict

    cursor.execute("""update nimbusio_central.collection
                   set access_control = %s
                   where name = %s""", [access_control, collection_name, ])

    collection_dict = {"success" : True}
    return httplib.OK, collection_dict

_dispatch_table = {"versioning"        : _set_collection_versioning,
                   "access_control"    : _set_collection_access_control }

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
                flask.abort(httplib.UNAUTHORIZED)

            cursor = connection.cursor()
            attribute = None
            status = None
            result_dict = None
            for key in flask.request.args:
                if key not in _dispatch_table:
                    error_message = "unknown attribute '{0}'".format(
                        flask.request.args)
                    log.error(error_message)
                    result_dict = {"success" : False,
                                   "error_message" : error_message}
                    status = httplib.METHOD_NOT_ALLOWED
                    break
                attribute = key
                break

            if attribute is not None:
                try:
                    status, result_dict = \
                        _dispatch_table[key](cursor, 
                                             collection_name, 
                                             flask.request.args[key])
                except Exception:
                    log.exception("{0} {1}".format(collection_name, 
                                                   attribute))
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

view_function = SetCollectionAttributeView.as_view(endpoint)

