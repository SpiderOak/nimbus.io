"""
"""

import os

_config_filename = "config.json"

class ClusterConfig(object):
    """
    Hold configuration state for cluster
    """
    def __init__(self, args):
        self.config = args
        self.base_ports = dict(
            writer =                    args.baseport,
            reader =                    1 * args.nodecount + args.baseport,
            anti_entropy =              2 * args.nodecount + args.baseport, 
            anti_entropy_pipeline =     3 * args.nodecount + args.baseport, 
            handoff =                   4 * args.nodecount + args.baseport, 
            handoff_pipeline =          5 * args.nodecount + args.baseport,
            event_publisher_pub  =      6 * args.nodecount + args.baseport,
            event_aggregator_pull =     7 * args.nodecount + args.baseport,
            # these two only need single ports
            space_accounting =          1 + 8 * args.nodecount + args.baseport,
            space_accounting_pipeline = 2 + 8 * args.nodecount + args.baseport, 
        )

    def env_for_cluster(self):
        "dict of ENV for the cluster as a whole"

        cluster_env = dict(
            PYTHONPATH = os.environ['PYTHONPATH'],
            NIMBUSIO_LOG_DIR = self.log_path,
            NIMBUSIO_CLUSTER_NAME = self.clustername,
            NIMBUSIO_NODE_NAME_SEQ = " ".join(self.node_names),
            NIMBUSIO_ANTI_ENTROPY_SERVER_ADDRESSES = \
                " ".join(self.anti_entropy_addresses),
            # TODO FIXME magic values
            NIMBUSIO_CENTRAL_USER_PASSWORD = "pork",
            NIMBUSIO_NODE_USER_PASSWORD    = "pork",
        )

        return cluster_env

    def env_for_node(self, node_index):
        "dict of ENV for a specific node in the cluster"
            #NIMBUSIO_ANTI_ENTROPY_SERVER_PIPELINE_ADDRESS =
            #    " ".join(self.anti_entropy_pipeline_addresses),
            #"NIMBUSIO_EVENT_PUBLISHER_PULL_ADDRESS" : \
        pass

    def __getattr__(self, name):
        if not hasattr(self.config, name):
            raise AttributeError(name)
        return getattr(self.config, name)

    @property
    def config_path(self):
        return os.path.join(self.basedir, _config_filename)

    @property
    def log_path(self):
        return os.path.join(self.basedir, "logs")

    @property
    def socket_path(self):
        return os.path.join(self.basedir, "sockets")

    def _node_service_addresses(self, name):
        return [ "tcp://127.0.0.1:%s" % i
            for i in range(self.base_ports[name],
                           self.base_ports[name] + self.nodecount)]

    @property
    def required_paths(self):
        "paths to all all folders that should exist to run this cluster"
        return [ self.basedir, self.log_path, self.socket_path ]

    @property
    def port_range(self):
        "(lowest, highest, ) local ports used"
        return (min(self.base_ports.values()), max(self.base_ports.values()), )

    @property
    def node_names(self):
        return [self.nodenamepattern % i for i in range(1, self.nodecount + 1)]

    @property
    def space_accounting_server_address(self):
        return "tcp://127.0.0.1:%d" % ( max(self.base_ports.values()) 
                                        + self.nodecount, )

    @property
    def space_accounting_pipeline_address(self):
        return "tcp://127.0.0.1:%d" % ( max(self.base_ports.values()) 
                                        + self.nodecount + 1 )

    @property
    def event_publisher_pull_addresses(self):
        return [ 
            "ipc:///%s/%s.nimbusio-event-publisher.socket" % ( 
                self.socket_path, n, )
             for n in self.node_names ]

    @property
    def event_publisher_pub_addresses(self):
        return self._node_service_addresses('event_publisher_pub')

    @property
    def event_aggregator_pub_addresses(self):
        return self._node_service_addresses('event_aggregator_pull')

    @property
    def data_writer_addresses(self):
        return self._node_service_addresses('writer')

    @property
    def data_reader_addresses(self):
        return self._node_service_addresses('reader')

    @property
    def anti_entropy_addresses(self):
        return self._node_service_addresses('anti_entropy')

    @property
    def anti_entropy_pipeline_addresses(self):
        return self._node_service_addresses('anti_entropy')

    @property
    def handoff_server_addresses(self):
        return self._node_service_addresses('handoff')

    @property
    def handoff_server_pipeline_addresses(self):
        return self._node_service_addresses('handoff_pipeline')
