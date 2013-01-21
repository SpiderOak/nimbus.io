# -*- coding: utf-8 -*-
"""
get_collection_attribute_view.py

A View to to get an attribute of a collection for a user
"""
from collections import namedtuple
import httplib
import json
import logging

import flask

from tools.greenlet_database_util import GetConnection
from tools.data_definitions import http_timestamp_str
from tools.customer_key_lookup import CustomerKeyConnectionLookup
from tools.collection import compute_default_collection_name

from web_collection_manager.connection_pool_view import ConnectionPoolView
from web_collection_manager.authenticator import authenticate

rules = ["/customers/<username>/collections/<collection_name>", ]
endpoint = "get_collection_attribute"

_memcached_space_accounting_template = \
    "nimbusio_space_accounting_{0}_{1}_{2}days" 
_short_day_query = """
SELECT 
        date_trunc('day', timestamp) as day,
        sum(retrieve_success),
        sum(archive_success),
        sum(listmatch_success),
        sum(delete_success),
        sum(success_bytes_in),
        sum(success_bytes_out)
  FROM nimbusio_central.collection_ops_accounting
 WHERE collection_id=%s
 GROUP BY day
 ORDER BY day desc
 LIMIT %s
"""

_long_day_query = """
SELECT 
        date_trunc('day', timestamp) as day,
        sum(retrieve_success),
        sum(archive_success),
        sum(listmatch_success),
        sum(delete_success),
        sum(success_bytes_in),
        sum(success_bytes_out)
  FROM (select * from collection_ops_accounting
        UNION ALL
        select * from collection_ops_accounting_old) combined
 WHERE collection_id=%s
 GROUP BY day
 ORDER BY day desc
 LIMIT %s
"""

_default_days_of_history = 7

_operational_stats_row = namedtuple("OperationalStatsRow", [
        "day",
        "retrieve_success",
        "archive_success",
        "listmatch_success",
        "delete_success",
        "success_bytes_in",
        "success_bytes_out"])

_expiration_time_in_seconds = 15 * 60 # expiration of 15 minutes

def _list_collection(cursor, customer_id, collection_name):
    """
    list a named collection for the customer
    """
    cursor.execute("""
        select name, versioning, access_control, creation_time 
        from nimbusio_central.collection   
        where customer_id = %s and name = %s and deletion_time is null
        """, [customer_id, collection_name])
    result = cursor.fetchone()

    return result

def _get_collection_info(cursor, username, customer_id, collection_name):
    """
    get basic information about the collection
    See Ticket #51 Implement GET JSON for a collection
    """
    log = logging.getLogger("_get_collection_info")
    log.debug("_list_collection(cursor, {0}, {1}".format(customer_id, 
                                                         collection_name))
    row = _list_collection(cursor, customer_id, collection_name)
    if row is None:
        collection_dict = {"success"       : False, 
                           "error_message" : "No such collection"}
        return httplib.NOT_FOUND, collection_dict

    default_collection_name = compute_default_collection_name(username)

    name, versioning, raw_access_control, raw_creation_time = row
    if raw_access_control is None:
        access_control = None
    else:
        access_control = json.loads(raw_access_control)
    collection_dict = {"success" : True,
                       "name" : name, 
                       "default_collection" : name == default_collection_name,
                       "versioning" : versioning, 
                       "access_control" : access_control,
                       "creation-time" : http_timestamp_str(raw_creation_time)}
    return httplib.OK, collection_dict

def _get_collection_id(cursor, customer_id, collection_name):
    """
    get the id of a named collection for the customer
    """
    cursor.execute("""
        select id
        from nimbusio_central.collection   
        where customer_id = %s and name = %s
        """, [customer_id, collection_name])
    result = cursor.fetchone()
    if result is None:
        return None

    return result[0]

def _get_collection_space_usage(memcached_client, 
                                cursor, 
                                customer_id, 
                                collection_name, 
                                args):
    """
    get usage information for the collection
    See Ticket #66 Include operational stats in API queries for space usage
    """
    log = logging.getLogger("_get_collection_space_usage")

    if "days_of_history" in args:
        # if N is specified, it is always rounded up to the nearest multiple 
        # of 30 (for caching)
        days_of_history = ((int(args["days_of_history"]) / 30) + 1) * 30
    else:
        days_of_history = _default_days_of_history
    log.debug("seeking {0} days of history".format(days_of_history))

    memcached_key = \
        _memcached_space_accounting_template.format(customer_id,
                                                    collection_name,
                                                    days_of_history)

    cached_dict = memcached_client.get(memcached_key)
    if cached_dict is not None:
        log.debug("cache hit {0} days {1}".format(
            len(cached_dict["operational_stats"]), memcached_key))
        return httplib.OK, cached_dict

    collection_id = _get_collection_id(cursor, customer_id, collection_name)
    if collection_id is None:
        collection_dict = {"success"       : False, 
                           "error_message" : "No such collection"}
        return httplib.NOT_FOUND, collection_dict

    # 2012-12-10 dougfort -- for reasons I don't understand, success_bytes_in
    # and success_bytes_out emerge as type Dec. So I force them to int to
    # keep JSON happy.
    
    if days_of_history > _default_days_of_history:
        cursor.execute(_long_day_query, [collection_id, days_of_history, ])
    else:
        cursor.execute(_short_day_query, [collection_id, days_of_history, ])

    collection_dict = {"success" : True, "operational_stats" : list()}
    for row in map(_operational_stats_row._make, cursor.fetchall()):
        stats_dict =  { "day" : http_timestamp_str(row.day),
            "retrieve_success" : row.retrieve_success,
            "archive_success"  : row.archive_success,
            "listmatch_success": row.listmatch_success,
            "delete_success"   : row.delete_success,
            "success_bytes_in" : int(row.success_bytes_in),
            "success_bytes_out": int(row.success_bytes_out), }
        collection_dict["operational_stats"].append(stats_dict)

    log.debug("database hit {0} days {1}".format(
        len(collection_dict["operational_stats"]), memcached_key))

    success = memcached_client.set(memcached_key, 
                                   collection_dict, 
                                   time=_expiration_time_in_seconds)
    if not success:
        log.error("memcached_client.set({0}...) returned {1}".format(
            memcached_key, success))

    return httplib.OK, collection_dict

class GetCollectionAttributeView(ConnectionPoolView):
    methods = ["GET", ]

    def dispatch_request(self, username, collection_name):
        log = logging.getLogger("GetCollectionAttributeView")
        user_request_id = flask.request.headers["x-nimbus-io-user-request-id"]
        log.info("user_request_id = {1}, " \
                 "user_name = {1}, " \
                 "collection_name = {1}".format(user_request_id,
                                                username, 
                                                collection_name))

        result_dict = None

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
            if "action" in  flask.request.args:
                assert flask.request.args["action"] == "space_usage"
                try:
                    status, result_dict = \
                        _get_collection_space_usage(self.memcached_client,
                                                    cursor, 
                                                    customer_id,
                                                    collection_name, 
                                                    flask.request.args)
                except Exception:
                    log.exception("user_request_id = {0}, " \
                                  "{1} {2}".format(user_request_id, 
                                                   collection_name, 
                                                   flask.request.args))
                    cursor.close()
                    raise
            else:
                try:
                    status, result_dict = _get_collection_info(cursor, 
                                                               username,
                                                               customer_id,
                                                               collection_name)
                except Exception:
                    log.exception("user_request_id = {0}, " \
                                  "{1} {2}".format(user_request_id,
                                                   collection_name, 
                                                   flask.request.args))
                    cursor.close()
                    raise
            cursor.close()


        # Ticket #33 Make Nimbus.io API responses consistently JSON
        data = json.dumps(result_dict, sort_keys=True, indent=4) 

        response = flask.Response(data, 
                                  status=status,
                                  content_type="application/json")
        response.headers["content-length"] = str(len(data))
        return response

view_function = GetCollectionAttributeView.as_view(endpoint)

