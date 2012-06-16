"""
very basic prototype for web director for nimbus.io

all it does so far is try to get traffic to one of the hosts in the appropriate
cluster

does not implement several of the needed capabilities including:
    - health checks for hosts or listen for a heartbeat
    - distinguish between read and write requests and send to different
      destinations
    - pick read hosts based on key consistent hashing for caching
    - pick write hosts based on least busy (busyness info included in
      heartbeats)
    - handle key authentication for reads
    - handle requests for public collections w/o auth
    - expiration of cached values (i.e. when a collections information changes,
      such as becoming public, private, or being migrated to a different
      cluster.)
"""

from functools import wraps
import time
import logging
import os
import re
import random
import gevent.coros
import psycopg2
import gevent_psycopg2
gevent_psycopg2.monkey_patch()

from tools.LRUCache import LRUCache
from tools.database_connection import get_central_connection

def _supervise_db_interaction(bound_method):
    """
    Decorator for methods of Router class to manage locks and reconnections to
    database
    """
    @wraps(bound_method)
    def __supervise_db_interaction(instance, *args, **kwargs):
        log = logging.getLogger("supervise_db")
        conn_id = id(instance.conn)
        lock = instance.dblock
        retries = 0
        start_time = time.time()

        # it maybe that some other greenlet has got here first, and already
        # updated our cache to include the item that we are querying. In some
        # situations, such as when the database takes a few seconds to respond,
        # or when the database is offline, there maybe many greenlets waiting,
        # all to query the database for the same result.  To avoid this
        # thundering herd of database hits of likely cached values, the caller
        # may supply us with a cache check function.
        cache_check_func = None
        if cache_check_func in kwargs:
            cache_check_func = kwargs.pop('cache_check_func') 

        while True:
            if retries:
                # do not retry too fast
                gevent.sleep(1.0)
            try:
                with lock:
                    if cache_check_func is not None:
                        result = cache_check_func()
                        if result:
                            break
                    result = bound_method(*args, **kwargs)
                    break
            except psycopg2.OperationalError, err:
                log.warn("Database error %s %s (retry #%d)" % (
                    getattr(err, "pgcode", '-'),
                    getattr(err, "pgerror", '-'),
                    retries, ))
                retries += 1
                with lock:
                    # only let one greenlet be retrying the connection
                    # only reconnect if some other greenlet hasn't already done
                    # so.
                    if id(instance.conn) == conn_id:
                        log.warn("replacing database connection %r" % (
                            conn_id, ))
                        try:
                            instance.conn = get_central_connection()
                            conn_id = id(instance.conn)
                        except psycopg2.OperationalError, err2:
                            log.warn("could not reconnect: %s %s" % ( 
                                getattr(err, "pgcode", '-'),
                                getattr(err, "pgerror", '-'), ))
        return result
    return __supervise_db_interaction

class Router(object):

    def __init__(self):
        self.conn = get_central_connection()
        self.dblock = gevent.coros.RLock()
        self.service_domain = os.environ['NIMBUSIO_SERVICE_DOMAIN']
        self.known_clusters = dict()
        # LRUCache mapping names to integers is approximately 32m of memory per
        # 100,000 entries
        self.known_collections = LRUCache(500000) 

    def parse_collection(self, hostname):
        "return the Nimbus.io collection name from host name"
        offset = -1 * ( len(self.service_domain) + 1 )
        return hostname[:offset]

    def hosts_for_collection(self, collection):
        "return a list of hosts for this collection"
        cluster_id = self.cluster_for_collection(collection)
        if cluster_id is None:
            return None
        cluster_info = self.cluster_info(cluster_id)
        return cluster_info['hosts']

    @_supervise_db_interaction
    def _db_cluster_for_collection(self, collection):
        # FIXME how do we handle null result here? do we just cache the null
        # result?
        row = self.conn.fetch_one_row(
            "select * from nimbusio_central.collection where name=%s",
            [collection, ])
        return row

    @_supervise_db_interaction
    def cluster_for_collection(self, collection, _retries=0):
        "return cluster ID for collection"
        if collection in self.known_collections:
            return self.known_collections[collection]
        result = self._db_cluster_for_collection(collection,
            cache_check_func = 
                lambda: self.known_collections.get(collection, None))
        self.known_collections[collection] = result
        return result
            
    @_supervise_db_interaction
    def _db_cluster_info(self, cluster_id):
        rows = self.conn.fetch_all_rows(
            "select * from nimbusio_central.node where cluster_id=%s "
            "order by node_number_in_cluster", 
            [cluster_id, ])
    
        info = dict(hosts = [r['hostname'] for r in rows])
        return info

    def cluster_info(self, cluster_id):
        "return info about a cluster and its hosts"
        if cluster_id in self.known_clusters:
            return self.known_clusters[cluster_id]
        
        info = self._db_cluster_info(cluster_id, 
            cache_check_func=lambda: self.known_clusters.get(cluster_id, None))
        
        self.known_clusters[cluster_id] = info 
        return info

    @staticmethod
    def reject(code, reason):
        "return a go away response"
        response = "%d %s" % (code, reason, )
        return dict(close = response)

    def route(self, hostname):
        if not hostname.endswith(self.service_domain):
            return self.reject(404, "Not found")
        collection = self.parse_collection(hostname)
        if collection is None:
            self.reject(404, "Collection not found")
        hosts = self.hosts_for_collection(collection)
        if hosts:
            return dict(remote=random.choice(hosts))
        elif hosts is None:
            self.reject(404, "Collection not found")
        return self.reject(500, "Retry later")

def proxy(data, _re_host=re.compile("Host:\s*(.*)\r\n"), _router=Router()):
    matches = _re_host.findall(data)
    if matches:
        hostname = matches.pop()
        return _router.route(hostname)
    elif len(data) > 4096:
        # we should have had a host header by now...
        return dict(close=True)
    else:
        # wait until we have more data
        return None
