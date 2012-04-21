# -*- coding: utf-8 -*-
"""
node_sim.py

simulate one node in a cluster
"""
import logging
import os
import os.path
import pickle

from tools.id_translator import _KEY_SIZE

class SimError(Exception):
    pass

from test.nimbusio_sim.process_util import start_web_server, \
        start_event_aggregator, \
        start_event_subscriber, \
        start_data_writer, \
        start_data_reader, \
        start_space_accounting_server, \
        start_handoff_server, \
        start_event_publisher, \
        start_performance_packager, \
        start_stats_subscriber, \
        poll_process, \
        terminate_process

def _create_id_translator_keys():
    return {
        "key"       : os.urandom(_KEY_SIZE),
        "hmac_key"  : os.urandom(_KEY_SIZE),
        "iv_key"    : os.urandom(_KEY_SIZE),
        "hmac_size" : 16,
    }

class NodeSim(object):
    """simulate one node in a cluster"""

    def __init__(
        self, 
        node_index, 
        cluster_config,
        space_accounting=False,
        performance_packager=False,
        event_aggregator=False,
        web_server=False,
        event_subscriber=False,
        stats_subscriber=False
    ):
        self._node_index = node_index
        self._cluster_config = cluster_config
        self._log = logging.getLogger(self.node_name)
        self._home_dir = os.path.join(
            self._cluster_config.basedir, self.node_name
        )
        if not os.path.exists(self._home_dir):
            os.makedirs(self._home_dir)
        id_translator_keys_path = os.path.join(
            self._home_dir, "id_translator_keys.pkl"
        )
        if not os.path.exists(id_translator_keys_path):
            id_translator_keys = _create_id_translator_keys()
            with open(id_translator_keys_path, "w") as output_file:
                pickle.dump(id_translator_keys, output_file)

        self._processes = dict()
        self._space_accounting = space_accounting
        self._performance_packager = performance_packager
        self._event_aggregator = event_aggregator
        self._web_server = web_server
        self._event_subscriber = event_subscriber
        self._stats_subscriber = stats_subscriber

    def __str__(self):
        return self.node_name

    def node_config(self, name):
        "return the named config value for this node in the cluster"
        return getattr(self._cluster_config, name)[self._node_index]

    @property
    def node_name(self):
        "shorthand for node_config('node_names')"
        return self.node_config('node_names')

    @property
    def env(self):
        "dict of ENV for the cluster and this node"
        return dict(self._cluster_config.env_for_cluster() +
                    self._cluster_config.env_for_node(self._node_index))

    def start(self):
        self._log.debug("start")

        self._processes["event_publisher"] = start_event_publisher(
            self.node_name, 
            self.env
        )

        self._processes["data_reader"] = start_data_reader(
            self.node_name, 
            self.env,
            self._cluster_config.profile
        )

        self._processes["data_writer"] = start_data_writer(
            self.node_name,
            self.env,
            self._cluster_config.profile
        )

        self._processes["handoff_server"] = start_handoff_server(
            self.node_name,
            self.env,
            self._cluster_config.profile
        )

        if self._space_accounting:
            self._processes["space_accounting"] = \
                start_space_accounting_server(self.node_name, self.env)

        if self._event_aggregator:
            self._processes["event_aggregator"] = \
                start_event_aggregator(self.env)

        if self._event_subscriber:
            self._processes["event_subscriber"] = \
                start_event_subscriber(self.env)

        if self._stats_subscriber:
            self._processes["stats_subscriber"] = \
                start_stats_subscriber(self.env)

        if self._performance_packager:
            self._processes["performance-packager"] = \
                start_performance_packager(self.node_name, self.env)

        if self._web_server:
            self._processes["web_server"] = \
                start_web_server(
                    self.node_name, 
                    self.env,
                    self._cluster_config.profile
                )

    def stop(self):
        self._log.debug("stop")
        for process_name in self._processes.keys():
            process = self._processes[process_name]
            if process is not None:
                self._log.debug("terminate process %s" % (process_name, ))
                terminate_process(process)
                self._processes[process_name] = None

    def poll(self):
        """check the condition of running processes"""
        self._log.debug("polling")

        for process_name in self._processes.keys():
            process = self._processes[process_name]
            if process is not None:
                result = poll_process(process)
                if result is not None:
                    returncode, stderr_content = result
                    error_string = "%s terminated abnormally (%s) '%s'" % (
                        process_name,
                        returncode, 
                        stderr_content,
                    )
                    self._log.error(error_string)
                    self._processes[process_name] = None

