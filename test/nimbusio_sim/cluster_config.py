"""
"""

import os

from tools.database_connection import \
    central_database_name, central_database_user, \
    node_database_name_prefix, node_database_user_prefix

_config_filename = "config.json"

class ClusterConfig(object):
    """
    Hold configuration state for cluster
    """
    def __init__(self, args):
        self.config = args
        self.database_users = dict()
        self.base_ports = dict(
            writer =                    args.baseport,
            reader =                    1 * args.nodecount + args.baseport,
            anti_entropy =              2 * args.nodecount + args.baseport, 
            anti_entropy_pipeline =     3 * args.nodecount + args.baseport, 
            handoff =                   4 * args.nodecount + args.baseport, 
            handoff_pipeline =          5 * args.nodecount + args.baseport,
            event_publisher_pub  =      6 * args.nodecount + args.baseport,
            event_aggregator_pull =     7 * args.nodecount + args.baseport,
            postgres =                  8 * args.nodecount + args.baseport,
            # these only need single ports
            postgres_central =          1 + 9 * args.nodecount + args.baseport, 
            event_aggregator_pub =      2 + 9 * args.nodecount + args.baseport, 
            space_accounting =          3 + 9 * args.nodecount + args.baseport,
            space_accounting_pipeline = 4 + 9 * args.nodecount + args.baseport, 
            web_server =                5 + 9 * args.nodecount + args.baseport, 
        )

    def env_for_cluster(self):
        "list of names and values for overall cluster"

        cluster_env = [
            ( "PYTHONPATH",
                os.environ['PYTHONPATH'], ),
            ( "NIMBUSIO_LOG_DIR",
                self.log_path, ),
            ( "NIMBUSIO_CLUSTER_NAME",
                self.clustername, ),
            ( "NIMBUSIO_NODE_NAME_SEQ",
                " ".join(self.node_names), ),
            ( "NIMBUSIO_ANTI_ENTROPY_SERVER_ADDRESSES",
                " ".join(self.anti_entropy_addresses), ),
            ( "NIMBUSIO_CENTRAL_USER_PASSWORD",       
                self.central_db_user, ),
            ( "NIMBUSIO_CENTRAL_DATABASE_HOST",
                self.dbhost, ),
            ( "NIMBUSIO_CENTRAL_DATABASE_PORT",       
                str(self.central_db_port), ),
            ( "NIMBUSIO_HANDOFF_SERVER_ADDRESSES", 
                " ".join(self.handoff_server_addresses), ),
            ( "NIMBUSIO_DATA_READER_ADDRESSES", 
                " ".join(self.data_reader_addresses), ),
            ( "NIMBUSIO_DATA_WRITER_ADDRESSES", 
                " ".join(self.data_writer_addresses), ),
        ]

        return cluster_env

    def env_for_node(self, node_index):
        "list of names and values for ENV for a specific node in the cluster"
        node_env = [
           ( "NIMBUSIO_NODE_NAME", 
                self.node_names[node_index], ),
           ( "NIMBUSIO_NODE_DATABASE_HOST", 
                self.dbhost, ),
           ( "NIMBUSIO_NODE_DATABASE_PORT", 
                str(self.node_db_ports[node_index]), ),
           ( "NIMBUSIO_NODE_DATABASE_USER", 
                self.node_db_users[node_index], ),
           ( "NIMBUSIO_NODE_DATABASE_NAME", 
                self.node_db_names[node_index], ),
           ( "NIMBUSIO_NODE_USER_PASSWORD", 
                self.node_db_pws[node_index], ),
           ( "NIMBUSIO_DATA_READER_ADDRESS", 
                self.data_reader_addresses[node_index], ),
           ( "NIMBUSIO_DATA_WRITER_ADDRESS",    
                self.data_writer_addresses[node_index], ),
           ( "NIMBUSIO_REPOSITORY_PATH",      
                self.node_repository_paths[node_index], ),
           ( "NIMBUSIO_HANDOFF_SERVER_PIPELINE_ADDRESS", 
                self.handoff_server_pipeline_addresses[node_index], ),
           ( "NIMBUSIO_EVENT_PUBLISHER_PULL_ADDRESS",
                self.event_publisher_pull_addresses[node_index], ),
           ( "NIMBUSIO_ANTI_ENTROPY_SERVER_PIPELINE_ADDRESS",
                self.anti_entropy_pipeline_addresses[node_index], ),
           ( "NIMBUSIO_EVENT_PUBLISHER_PULL_ADDRESS",
                self.event_publisher_pull_addresses[node_index], ),
           ( "NIMBUSIO_SPACE_ACCOUNTING_SERVER_ADDRESS",
                self.space_accounting_server_address, ),
           ( "NIMBUSIO_SPACE_ACCOUNTING_PIPELINE_ADDRESS",
                self.space_accounting_pipeline_address, ),
           ( "NIMBUSIO_EVENT_PUBLISHER_PULL_ADDRESS",
                self.event_publisher_pull_addresses[node_index], ),
           ( "NIMBUSIO_EVENT_PUBLISHER_PUB_ADDRESS",
                self.event_publisher_pub_addresses[node_index], ),
           ( "NIMBUSIO_EVENT_AGGREGATOR_PUB_ADDRESS",
                self.event_aggregator_pub_address, ),
        ]

        return node_env

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
    def central_db_path(self):
        return os.path.join(self.basedir, "db", "central")

    @property
    def central_db_port(self):
        return self.base_ports['postgres_central']

    @property
    def central_db_name(self):
        return central_database_name

    @property
    def central_db_user(self):
        return central_database_user

    @property
    def central_db_pw(self):
        return self.database_users[self.central_db_user]

    @property
    def node_db_paths(self):
        return [
            os.path.join(self.basedir, "db", n) for n in self.node_names]

    @property
    def node_db_ports(self):
        return range(
            self.base_ports['postgres'], 
            self.base_ports['postgres'] + self.nodecount)

    @property
    def node_db_names(self):
        return [".".join([node_database_name_prefix, name]) 
                for name in self.node_names]

    @property
    def node_db_users(self):
        return [".".join([node_database_user_prefix, name]) 
                for name in self.node_names]

    @property
    def node_db_pws(self):
        return [self.database_users[u] for u in self.node_db_users]

    @property
    def socket_path(self):
        return os.path.join(self.basedir, "sockets")

    def _node_service_addresses(self, name):
        return [ "tcp://%s:%s" % (self.ip, i, )
            for i in range(self.base_ports[name],
                           self.base_ports[name] + self.nodecount)]

    @property
    def required_paths(self):
        "paths to all all folders that should exist to run this cluster"
        return [ self.basedir, self.log_path, self.socket_path ]

    @property
    def node_repository_paths(self):
        return [os.path.join(self.basedir, n) for n in self.node_names]

    @property
    def port_range(self):
        "(lowest, highest, ) local ports used"
        return (min(self.base_ports.values()), max(self.base_ports.values()), )

    @property
    def node_names(self):
        return [self.nodenamepattern % i for i in range(1, self.nodecount + 1)]

    @property
    def space_accounting_server_address(self):
        return "tcp://%s:%d" % ( 
            self.ip, self.base_ports['space_accounting'], )

    @property
    def space_accounting_pipeline_address(self):
        return "tcp://%s:%d" % ( 
            self.ip, self.base_ports['space_accounting_pipeline'], )

    @property
    def web_server_url(self):
        return "http://%s:%d" % ( self.ip, self.base_ports['web_server'], )

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
    def event_aggregator_pub_address(self):
        return "tcp://%s:%d" % (
            self.ip, self.base_ports['event_aggregator_pub'], )

    @property
    def event_aggregator_pull_addresses(self):
        return self._node_service_addresses('event_aggregator_pull')

    @property
    def event_aggregator_addresses(self):
        return self._node_service_addresses('event_aggregator')

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
