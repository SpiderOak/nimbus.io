"""
very basic prototype for web director for nimbus.io

all it does so far is try to get traffic to one of the hosts in the appropriate
cluster

does not implement several of the needed capabilities including:
    - health checks for hosts or listen for a heartbeat
    - distinguish between read and write requests and send to different
      destinations
    - pick read hosts based on key consistent hashing for caching
    - pick write hosts based on least busy (business info included in
      heartbeats)
    - handle key authentication for reads
    - handle requestts for public collections w/o auth
    - expiration of cached values (i.e. when a collecitons information changes,
      such as becoming public, private, or being migrated to a different
      cluster.)
"""

import re
import random
import gevent.coros
from tools.LRUCache import LRUCache
from tools.database_connection import get_central_connection

class Router(object):

    def __init__(self):
        self.conn = get_central_connection()
        self.dblock = gevent.coros.RLock()
        self.service_domain = os.environ['NIMBUSIO_SERVICE_DOMAIN']
        self.known_clusters = dict()
        self.known_collections = LRUCache(500000)

    def parse_collection(self, hostname):
        "return the Nimbus.io collection name from host name"
        offset = -1 * ( len(self.service_domain) + 1 )
        return hostname[:offset]

    def hosts_for_collection(self, collection):
        "return a list of hosts for this collection"
        cluster_id = self.cluster_for_collection(collection)
        cluster_info = self.cluster_info(cluster_id)
        return cluster_info['hosts']

    def cluster_for_collection(self, collection):
        "return cluster ID for collection"
        if collection in self.known_collections:
            return self.known_collections[collection]

        with self.dblock:
            row = self.conn.fetch_one_row(
                "select * from nimbusio_central.collection where name=%s",
                [collection, ])
            if row is not None:
                self.known_collections[name] = row

        return row
            
    def cluster_info(self, cluster_id):
        "return info about a cluster and its hosts"
        if cluster_id in self.known_clusters:
            return self.known_clusters[cluster_id]

        with self.dblock:
            rows = self.conn.fetch_all_rows(
                "select * from nimbusio_central.node where cluster_id=%s "
                "order by node_number_in_cluster", 
                [cluster_id, ])
        
        info = dict(hosts = [r['hostname'] for r in rows])
        
        self.known_clusters[cluster_id] = info 
        return info

    def reject(self, code, reason):
        "return a go away response"
        response = "%d %s" % (code, reason, )
        return dict(close = response)

    def route(self, hostname):
        if not hostname.endswith(self.service_domain):
            return self.reject(404, "Not found")
        collection = self.parse_collection(hostname)
        if collection is None:
            self.reject(404, "Collection not found")
        hosts = hosts_for_collection(hostname)
        if hosts:
            return dict(remote=random.choice(hosts))
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
