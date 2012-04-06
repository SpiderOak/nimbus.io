"""
"""

import os
import json
from argparse import Namespace

from tools.database_connection import \
    central_database_name, central_database_user, \
    node_database_name_prefix, node_database_user_prefix

_CONFIG_FILENAME = "config.json"

_ENV_CONSTANTS = [
    # web server
    ( "NIMBUS_IO_REPLY_TIMEOUT", "900", ),
    ( "NIMBUS_IO_WSGI_BACKLOG", "1024", ),
    ( "NIMBUS_IO_SLICE_SIZE", str(10 * 1024 * 1024), ),
    # data writer
    ( "NIMBUS_IO_MAX_VALUE_FILE_SIZE", str(1024 * 1024 * 1024), ),
    ( "NIMBUS_IO_SYNC_STRATEGY", "NONE"), # NONE, O_SYNC
]

class ClusterConfig(object):
    """
    Hold configuration state for cluster
    """
    def __init__(self, args):
        self.config = args
        self.database_users = dict()
        self._calculate_base_ports()

    def _calculate_base_ports(self):
        args = self.config
        self.base_ports = dict(
            web_server =                args.baseport, 
            writer =                    1 * args.nodecount + args.baseport,
            reader =                    2 * args.nodecount + args.baseport,
            anti_entropy =              3 * args.nodecount + args.baseport, 
            anti_entropy_pipeline =     4 * args.nodecount + args.baseport, 
            handoff =                   5 * args.nodecount + args.baseport, 
            handoff_pipeline =          6 * args.nodecount + args.baseport,
            event_publisher_pub  =      7 * args.nodecount + args.baseport,
            event_aggregator_pull =     8 * args.nodecount + args.baseport,
            postgres =                  9 * args.nodecount + args.baseport,
            web_server_pipeline =      10 * args.nodecount + args.baseport,
            # these only need single ports
            postgres_central =          1 + (11 * args.nodecount) + args.baseport, 
            event_aggregator_pub =      2 + (11 * args.nodecount) + args.baseport, 
            space_accounting =          3 + (11 * args.nodecount) + args.baseport,
            space_accounting_pipeline = 4 + (11 * args.nodecount) + args.baseport, 
            cluster_inspector_pipeline= 5 + (11 * args.nodecount) + args.baseport, 
        )

    def __getstate__(self):
        "make this storable by JSON for easy reading/editing by humans"
        saved_state =  dict(config=self.config.__dict__,
                            database_users=self.database_users)
        # createnew is always false when saving state
        saved_state['config']['createnew'] = False
        return saved_state

    def __setstate__(self, saved_state):
        "convert JSON store back to python"
        self.config = Namespace(**saved_state['config'])
        self.database_users = saved_state['database_users']
        self._calculate_base_ports()

    def env_for_clients(self):
        "env for clients and tests running against this simulated cluster"

        client_env = [
            # host/port: will be the varnish or nginx address when we integrate
            # that. for now, it's just the first webserver.
            ( "NIMBUS_IO_SERVICE_PORT",
                str(self.web_server_ports[0]), ),
            ( "NIMBUS_IO_SERVICE_HOST",
                self.ip, ),
            ( "NIMBUS_IO_SERVICE_DOMAIN",
                "sim.nimbus.io", ), 
            # until we integrate nginx in the simulator, test clusters don't
            # support SSL
            ( "NIMBUS_IO_SERVICE_SSL",
                "0", ), 
            ( "USE_BOTO", 
                "0", ), 
            ( "NIMBUS_IO_CLIENT_PATH", 
                self.client_path, ),
        ]
        return client_env

    def env_for_cluster(self):
        "list of names and values for overall cluster"

        cluster_env = [
            ( "PYTHONPATH",
                os.environ['PYTHONPATH'], ),
            # run on simulation domain: sim.nimbus.io and 
            # *.sim.nimbus.io resolve to 127.0.0.1
            ( "NIMBUS_IO_SERVICE_DOMAIN",
                "sim.nimbus.io", ), 
            ( "NIMBUSIO_LOG_DIR",
                self.log_path, ),
            ( "NIMBUSIO_PROFILE_DIR",
                self.profile_path, ),
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
            ( "NIMBUSIO_NODE_DATABASE_HOSTS", 
                " ".join([self.dbhost for _ in range(len(self.node_names))]),),
            ( "NIMBUSIO_NODE_DATABASE_PORTS", 
                " ".join([str(port) for port in self.node_db_ports]), ),
            ( "NIMBUSIO_HANDOFF_SERVER_ADDRESSES", 
                " ".join(self.handoff_server_addresses), ),
            ( "NIMBUSIO_DATA_READER_ADDRESSES", 
                " ".join(self.data_reader_addresses), ),
            ( "NIMBUSIO_DATA_WRITER_ADDRESSES", 
                " ".join(self.data_writer_addresses), ),
            ( "NIMBUSIO_EVENT_PUBLISHER_PUB_ADDRESSES",
                " ".join(self.event_publisher_pub_addresses), ),
        ]

        return _ENV_CONSTANTS + cluster_env

    def env_for_node(self, node_index):
        "list of names and values for ENV for a specific node in the cluster"
        node_env = [
           ( "NIMBUSIO_NODE_NAME", 
                self.node_names[node_index], ),
           ( "NIMBUSIO_REPOSITORY_PATH",      
                self.node_repository_paths[node_index], ),
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
           ( "NIMBUSIO_WEB_SERVER_HOST",
                self.ip, ),
           ( "NIMBUSIO_WEB_SERVER_PORT",
                str(self.web_server_ports[node_index]), ),
           ( "NIMBUSIO_WEB_SERVER_PIPELINE_ADDRESS",
                self.web_server_pipeline_addresses[node_index], ),
           ( "NIMBUSIO_DATA_READER_ADDRESS", 
                self.data_reader_addresses[node_index], ),
           ( "NIMBUSIO_DATA_WRITER_ADDRESS",    
                self.data_writer_addresses[node_index], ),
           ( "NIMBUSIO_HANDOFF_SERVER_PIPELINE_ADDRESS", 
                self.handoff_server_pipeline_addresses[node_index], ),
           ( "NIMBUSIO_ANTI_ENTROPY_SERVER_PIPELINE_ADDRESS",
                self.anti_entropy_pipeline_addresses[node_index], ),
           ( "NIMBUSIO_EVENT_PUBLISHER_PULL_ADDRESS",
                self.event_publisher_pull_addresses[node_index], ),
           ( "NIMBUSIO_EVENT_PUBLISHER_PUB_ADDRESS",
                self.event_publisher_pub_addresses[node_index], ),
           ( "NIMBUSIO_SPACE_ACCOUNTING_SERVER_ADDRESS",
                self.space_accounting_server_address, ),
           ( "NIMBUSIO_SPACE_ACCOUNTING_PIPELINE_ADDRESS",
                self.space_accounting_pipeline_address, ),
           ( "NIMBUSIO_EVENT_AGGREGATOR_PUB_ADDRESS",
                self.event_aggregator_pub_address, ),
        ]

        return node_env

    def __getattr__(self, name):
        if not hasattr(self.config, name):
            raise AttributeError(name)
        return getattr(self.config, name)

    @property
    def config_dir(self):
        return os.path.join(self.basedir, "config")

    @property
    def config_path(self):
        return os.path.join(self.config_dir, _CONFIG_FILENAME)

    def save(self):
        with open(self.config_path, "wb") as fobj:
            json.dump(self.__getstate__(), fobj, indent=4, sort_keys=True)
        self.write_config_scripts()
         
    @classmethod
    def load(cls, config):
        new_config = ClusterConfig(config)
        saved_config = json.load(open(new_config.config_path))
        new_config.__setstate__(saved_config)
        return new_config

    def write_config_scripts(self):
        """
        write bash scripts showing ENV settings for cluster and nodes
        for easily running tests, building additional services, etc.
        """

        # for clients/tests
        client_config_filename = os.path.join(self.config_dir, 
            "client_config.sh")
        with open(client_config_filename, "wb") as fobj:
            fobj.write("#!/bin/bash\n\n")
            fobj.write("# this file is autogenerated\n")
            for k, v in self.env_for_clients():
                fobj.write('export %s="%s"\n' % ( k, v, ))

        # central
        central_config_filename = os.path.join(self.config_dir, 
            "central_config.sh")

        # eeach node
        with open(central_config_filename, "wb") as fobj:
            fobj.write("#!/bin/bash\n\n")
            fobj.write("# this file is autogenerated\n")
            for k, v in self.env_for_cluster():
                fobj.write('export %s="%s"\n' % ( k, v, ))

        for n in range(self.nodecount):
            filename = os.path.join(self.config_dir, 
                "node_%02d_config.sh" % (n + 1, ))
            with open(filename, "wb") as fobj:
                fobj.write("#!/bin/bash\n\n")
                fobj.write("# this file is autogenerated\n")
                fobj.write("source central_config.sh\n")
                for k, v in self.env_for_node(n):
                    fobj.write('export %s="%s"\n' % ( k, v, ))

    @property
    def log_path(self):
        return os.path.join(self.basedir, "logs")

    @property
    def profile_path(self):
        return os.path.join(self.basedir, "profile")

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
        if self.singledb:
            return [self.central_db_path for n in self.node_names]
        return [
            os.path.join(self.basedir, "db", n) for n in self.node_names]

    @property
    def node_db_ports(self):
        if self.singledb:
            return [self.central_db_port for n in self.node_names]

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
    def client_path(self):
        "path to client/user config files and client data (for tests, etc.)"
        return os.path.join(self.basedir, "client")

    @property
    def required_paths(self):
        "paths to all all folders that should exist to run this cluster"
        return [ self.basedir, self.log_path, self.profile_path, 
                 self.socket_path, self.config_dir, self.client_path, ]

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
    def web_server_pipeline_addresses(self):
        return self._node_service_addresses('web_server_pipeline')

    @property
    def web_server_ports(self):
        return [n + self.base_ports['web_server']
                for n in range(self.nodecount)]

    @property
    def web_server_urls(self):
        return [ 'http://%s:%d/' % ( self.ip, p, ) 
                 for p in self.web_server_ports]

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
        return self._node_service_addresses('anti_entropy_pipeline')

    @property
    def handoff_server_addresses(self):
        return self._node_service_addresses('handoff')

    @property
    def handoff_server_pipeline_addresses(self):
        return self._node_service_addresses('handoff_pipeline')
