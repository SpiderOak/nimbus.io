# -*- coding: utf-8 -*-
"""
node_sim.py

simulate one node in a cluster
"""
import logging
import os
import os.path

class SimError(Exception):
    pass

from unit_tests.util import start_event_aggregator, \
        start_data_writer, \
        start_data_reader, \
        start_data_writer, \
        start_space_accounting_server, \
        start_handoff_server, \
        start_anti_entropy_server, \
        start_event_publisher, \
        start_performance_packager, \
        poll_process, \
        terminate_process

#def _generate_node_name(node_index):
#    return "multi-node-%02d" % (node_index+1, )
#
#_test_base_path = os.environ["TEST_BASE_PATH"]
#_cluster_name = "multi-node-cluster"
#_node_count = 10
#_data_writer_base_port = 8100
#_data_reader_base_port = 8300
#_space_accounting_server_address = "tcp://127.0.0.1:8500"
#_space_accounting_pipeline_address = "tcp://127.0.0.1:8550"
#_anti_entropy_server_base_port = 8600
#_handoff_server_base_port = 8700
#_event_publisher_base_port = 8800
#_event_aggregator_base_port = 8900
#_data_writer_addresses = [
#    "tcp://127.0.0.1:%s" % (_data_writer_base_port+i, ) \
#    for i in range(_node_count)
#]
#_data_reader_addresses = [
#    "tcp://127.0.0.1:%s" % (_data_reader_base_port+i, ) \
#    for i in range(_node_count)
#]
#_anti_entropy_server_addresses = [
#
#    "tcp://127.0.0.1:%s" % (_anti_entropy_server_base_port+i, ) \
#    for i in range(_node_count)
#]
#_anti_entropy_server_pipeline_addresses = [
#    "tcp://127.0.0.1:%s" % (_anti_entropy_server_base_port+50+i, ) \
#    for i in range(_node_count)
#]
#_handoff_server_addresses = [
#    "tcp://127.0.0.1:%s" % (_handoff_server_base_port+i, ) \
#    for i in range(_node_count)
#]
#_handoff_server_pipeline_addresses = [
#    "tcp://127.0.0.1:%s" % (_handoff_server_base_port+50+i, ) \
#    for i in range(_node_count)
#]
#_node_names = [_generate_node_name(i) for i in range(_node_count)]
#_event_publisher_pull_addresses = [
#    "ipc:///tmp/nimbusio-event-publisher-%s/socket" % (node_name, ) \
#    for node_name in _node_names
#]
#_event_publisher_pub_addresses = [
#    "tcp://127.0.0.1:%s" % (_event_publisher_base_port+i, ) \
#    for i in range(_node_count)
#]
#_event_aggregator_pub_address = "tcp://127.0.0.1:%s" % (
#    _event_aggregator_base_port 
#)

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
    ):
        self._node_index = node_index
        self._cluster_config = cluster_config
        self._log = logging.getLogger(self.node_name)
        self._home_dir = os.path.join(self._cluster_config.basedir, self.node_name)
        if not os.path.exists(self._home_dir):
            os.makedirs(self._home_dir)

        self._processes = dict()
        self._space_accounting = space_accounting
        self._performance_packager = performance_packager
        self._event_aggregator = event_aggregator

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
            None, None,
            environment = self.env
        )

        self._processes["data_reader"] = start_data_reader(
            self.node_name, 
            None, None, None,
            environment = self.env

        )

        self._processes["data_writer"] = start_data_writer(
            self._cluster_config.clustername,
            self.node_name,
            None, None, None,
            environment = self.env
        )

        self._processes["handoff_server"] = start_handoff_server(
            self._cluster_config.clustername,
            self.node_name,
            None, None, None, None, None, None,
            environment = self.env
        )

        self._processes["anti_entropy_server"] = start_anti_entropy_server(
            None, None,
            self.node_name,
            None, None, None,
            environment = self.env
        )

        if self._space_accounting:
            self._processes["space_accounting"] = \
                start_space_accounting_server(
                    self.node_name,
                    None, None, None,
                    environment = self.env
                )

        if self._event_aggregator:
            self._processes["event_aggregator"] = \
                start_event_aggregator(
                    None, None, None, 
                    environment = self.env
                )

        if self._performance_packager:
            self._processes["performance-packager"] = \
                start_performance_packager(
                    self.node_name,
                    None,
                    environment = self.env
                )

    def stop(self):
        self._log.debug("stop")
        for process_name in self._processes.keys():
            process = self._processes[process_name]
            if process is not None:
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

