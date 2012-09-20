import os
import time
import logging
from collections import deque
import gevent
from gevent.event import AsyncResult
import httplib
from redis import StrictRedis, RedisError
import socket
import json
import memcache

from gdbpool.interaction_pool import DBInteractionPool

from tools.LRUCache import LRUCache
from tools.database_connection import get_central_database_dsn
from tools.collection_lookup import CollectionLookup

# LRUCache mapping names to integers is approximately 32m of memory per 100,000
# entries

# how long to wait before returning an error message to avoid fast loops
RETRY_DELAY = 1.0
AVAILABILITY_TIMEOUT = 30.0

COLLECTION_CACHE_SIZE = 500000
CENTRAL_DB_POOL_SIZE = int(os.environ.get(
    "NIMBUS_IO_CENTRAL_DB_POOL_SIZE", "5"))

NIMBUS_IO_SERVICE_DOMAIN = os.environ['NIMBUS_IO_SERVICE_DOMAIN']
NIMBUSIO_WEB_PUBLIC_READER_PORT = \
    int(os.environ['NIMBUSIO_WEB_PUBLIC_READER_PORT'])
NIMBUSIO_WEB_WRITER_PORT = int(os.environ['NIMBUSIO_WEB_WRITER_PORT'])
NIMBUSIO_MANAGEMENT_API_REQUEST_DEST = \
    os.environ['NIMBUSIO_MANAGEMENT_API_REQUEST_DEST']

REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", str(6379)))
REDIS_DB = int(os.environ.get("REDIS_DB", str(0)))
REDIS_WEB_MONITOR_HASH_NAME = "nimbus.io.web_monitor.{0}".format(
    socket.gethostname())
REDIS_WEB_MONITOR_HASHKEY_FORMAT = "%s:%s"

MEMCACHED_HOST = os.environ.get("NIMBUSIO_MEMCACHED_HOST", "localhost")
MEMCACHED_PORT = int(os.environ.get("NIMBUSIO_MEMCACHED_PORT", "11211"))
MEMCACHED_NODES = ["{0}:{1}".format(MEMCACHED_HOST, MEMCACHED_PORT), ]


class Router(object):
    """
    Router object for assisting the proxy function (below.)
    Holds database connection, state for caching, etc.
    """

    def __init__(self):
        self.init_complete = AsyncResult()
        self.central_conn_pool = None
        self.redis = None
        self.service_domain = NIMBUS_IO_SERVICE_DOMAIN
        self.read_dest_port = NIMBUSIO_WEB_PUBLIC_READER_PORT
        self.write_dest_port = NIMBUSIO_WEB_WRITER_PORT
        self.known_clusters = dict()
        self.management_api_request_dest_hosts = \
            deque(NIMBUSIO_MANAGEMENT_API_REQUEST_DEST.strip().split())
        self.memcached_client = None
        self.collection_lookup = None
        self.request_counter = 0

    def init(self):
        #import logging
        #import traceback
        log = logging.getLogger("init")
        log.info("init start")

        self.central_conn_pool = DBInteractionPool(
            get_central_database_dsn(), 
            pool_size = CENTRAL_DB_POOL_SIZE, 
            do_log = True )


        self.redis = StrictRedis(host = REDIS_HOST,
                                 port = REDIS_PORT,
                                 db = REDIS_DB)

        self.memcached_client = memcache.Client(MEMCACHED_NODES)

        self.collection_lookup = CollectionLookup(self.memcached_client,
                                                  self.central_conn_pool)

        log.info("init complete")
        self.init_complete.set(True)

    def _parse_collection(self, hostname):
        "return the Nimbus.io collection name from host name"
        offset = -1 * ( len(self.service_domain) + 1 )
        return hostname[:offset].lower()

    def _hosts_for_collection(self, collection):
        "return a list of hosts for this collection"
        cluster_id = self._cluster_for_collection(collection)
        if cluster_id is None:
            return None
        cluster_info = self._cluster_info(cluster_id)
        return cluster_info['hosts']

    def _cluster_for_collection(self, collection, _retries=0):
        "return cluster ID for collection"

        collection_row = self.collection_lookup.get(collection)
        if not collection_row:
            return None
        return collection_row['cluster_id']
            
    def _db_cluster_info(self, cluster_id):
        async_result = self.central_conn_pool.run("""
            select name, hostname, node_number_in_cluster 
            from nimbusio_central.node 
            where cluster_id=%s 
            order by node_number_in_cluster""", 
            [cluster_id, ])

        rows = async_result.get()
    
        info = dict(rows = rows, 
                    hosts = deque([r['hostname'] for r in rows]))

        return info

    def _cluster_info(self, cluster_id):
        "return info about a cluster and its hosts"
        if cluster_id in self.known_clusters:
            return self.known_clusters[cluster_id]
        
        info = self._db_cluster_info(cluster_id)
        
        self.known_clusters[cluster_id] = info 
        return info

    def check_availability(self, hosts, dest_port, _resolve_cache=dict()):
        "return set of hosts we think are available" 
        log = logging.getLogger("check_availability")

        available = set()
        if not hosts:
            return available

        addresses = []
        for host in hosts:
            if not host in _resolve_cache:
                _resolve_cache[host] = socket.gethostbyname(host)
            addresses.append(_resolve_cache[host])

        redis_keys = [ REDIS_WEB_MONITOR_HASHKEY_FORMAT % (a, dest_port, )
                       for a in addresses ]

        try:
            redis_values = self.redis.hmget(REDIS_WEB_MONITOR_HASH_NAME,
                                            redis_keys)
        except RedisError as err:
            log.warn("redis error querying availability for %s: %s, %r"
                % ( REDIS_WEB_MONITOR_HASH_NAME, err, redis_keys, ))
            # just consider everything available. it's the best we can do.
            available.update(hosts)
            return available

        unknown = []
        for idx, val in enumerate(redis_values):
            if val is None:
                unknown.append((hosts[idx], redis_keys[idx], ))
                continue
            try:
                status = json.loads(val)
            except Exception, err:
                log.warn("cannot decode %s %s %s %r" % ( 
                    REDIS_WEB_MONITOR_HASH_NAME, hosts[idx], 
                    redis_keys[idx], val, ))
            else:
                if status["reachable"]:
                    available.add(hosts[idx])
            
        if unknown:
            log.warn("no availability info in redis for hkeys: %s %r" % 
                ( REDIS_WEB_MONITOR_HASH_NAME, unknown, ))
            # if every host is unknown, just consider them all available
            if len(unknown) == len(hosts):
                available.update(hosts)

        return available

    @staticmethod
    def _reject(code, reason=None):
        "return a go away response"
        log = logging.getLogger("reject")
        http_error_str = httplib.responses.get(code, "unknown")
        log.debug("reject: %d %s %r" % (code, http_error_str, reason, ))
        if reason is None:
            reason = http_error_str
        return { 'close': 'HTTP/1.0 %d %s\r\n\r\n%s' % ( 
                  code, http_error_str, reason, ) }

    def route(self, hostname, method, path, _query_string, start=None):
        """
        route a to a host in the appropriate cluster, using simple round-robin
        among the hosts in a cluster
        """
        log = logging.getLogger("route")

        self.init_complete.wait()

        self.request_counter += 1
        request_num = self.request_counter

        log.debug(
            "request %d: host=%r, method=%r, path=%r, query=%r, start=%r" %
            (request_num, hostname, method, path, _query_string, start))


        # TODO: be able to handle http requests from http 1.0 clients w/o a
        # host header to at least the website, if nothing else.
        if hostname is None or (not hostname.endswith(self.service_domain)):
            return self._reject(httplib.NOT_FOUND)

        if hostname == self.service_domain:
            # this is not a request specific to any particular collection
            # TODO figure out how to route these requests.
            # in production, this might not matter.
            self.management_api_request_dest_hosts.rotate(1)
            target = self.management_api_request_dest_hosts[0]
            log.debug("request %d to backend host %s" %
                (request_num, target, ))
            return dict(remote = target)

        # determine if the request is a read or write
        if method in ('POST', 'DELETE', 'PUT', 'PATCH', ):
            dest_port = self.write_dest_port
        elif method in ('HEAD', 'GET', ):
            dest_port = self.read_dest_port
        else:
            self._reject(httplib.BAD_REQUEST, "Unknown method")

        collection = self._parse_collection(hostname)
        if collection is None:
            self._reject(httplib.NOT_FOUND, "No such collection")

        hosts = self._hosts_for_collection(collection)

        if hosts is None:
            self._reject(httplib.NOT_FOUND, "No such collection")

        availability = self.check_availability(hosts, dest_port)    

        # find an available host
        for _ in xrange(len(hosts)):
            hosts.rotate(1)
            target = hosts[0]
            if target in availability:
                break
        else:
            # we never found an available host
            now = time.time()
            if start is None:
                log.warn("Request %d No available service, waiting..." %
                    (request_num, ))
                start = now
            if now - start > AVAILABILITY_TIMEOUT:
                return self._reject(httplib.SERVICE_UNAVAILABLE, "Retry later")
            gevent.sleep(RETRY_DELAY)
            return self.route(hostname, method, path, _query_string, start)

        log.debug("request %d to backend host %s port %d" %
            (request_num, target, dest_port, ))
        return dict(remote = "%s:%d" % (target, dest_port, ))

        # no hosts currently available (hosts is an empty list, presumably)
