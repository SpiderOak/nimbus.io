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

from unit_tests.util import start_data_writer, \
        start_data_reader, \
        start_data_writer, \
        start_space_accounting_server, \
        start_handoff_server, \
        start_anti_entropy_server, \
        start_event_publisher, \
        start_performance_packager, \
        poll_process, \
        terminate_process

def _generate_node_name(node_index):
    return "multi-node-%02d" % (node_index+1, )

_cluster_name = "multi-node-cluster"
_node_count = 10
_data_writer_base_port = 8100
_data_reader_base_port = 8300
_space_accounting_server_address = "tcp://127.0.0.1:8500"
_space_accounting_pipeline_address = "tcp://127.0.0.1:8550"
_anti_entropy_server_base_port = 8600
_handoff_server_base_port = 8700
_event_publisher_base_port = 8800
_data_writer_addresses = [
    "tcp://127.0.0.1:%s" % (_data_writer_base_port+i, ) \
    for i in range(_node_count)
]
_data_reader_addresses = [
    "tcp://127.0.0.1:%s" % (_data_reader_base_port+i, ) \
    for i in range(_node_count)
]
_anti_entropy_server_addresses = [
    "tcp://127.0.0.1:%s" % (_anti_entropy_server_base_port+i, ) \
    for i in range(_node_count)
]
_anti_entropy_server_pipeline_addresses = [
    "tcp://127.0.0.1:%s" % (_anti_entropy_server_base_port+50+i, ) \
    for i in range(_node_count)
]
_handoff_server_addresses = [
    "tcp://127.0.0.1:%s" % (_handoff_server_base_port+i, ) \
    for i in range(_node_count)
]
_handoff_server_pipeline_addresses = [
    "tcp://127.0.0.1:%s" % (_handoff_server_base_port+50+i, ) \
    for i in range(_node_count)
]
_node_names = [_generate_node_name(i) for i in range(_node_count)]
_event_publisher_pull_addresses = [
    "ipc:///tmp/spideroak-event-publisher-%s/socket" % (node_name, ) \
    for node_name in _node_names
]
_event_publisher_pub_addresses = [
    "tcp://127.0.0.1:%s" % (_event_publisher_base_port+i, ) \
    for i in range(_node_count)
]

class NodeSim(object):
    """simulate one node in a cluster"""

    def __init__(
        self, 
        test_dir, 
        node_index, 
        space_accounting=False,
        performance_packager=False
    ):
        self._node_index = node_index
        self._node_name = _generate_node_name(node_index)
        self._log = logging.getLogger(self._node_name)
        self._home_dir = os.path.join(test_dir, self._node_name)
        if not os.path.exists(self._home_dir):
            os.makedirs(self._home_dir)
        self._processes = dict()
        self._space_accounting = space_accounting
        self._performance_packager = performance_packager

    def __str__(self):
        return self._node_name

    def start(self):
        self._log.debug("start")

        self._processes["event_publisher"] = start_event_publisher(
            self._node_name, 
            _event_publisher_pull_addresses[self._node_index],
            _event_publisher_pub_addresses[self._node_index]
        )
        self._processes["data_reader"] = start_data_reader(
            self._node_name, 
            _data_reader_addresses[self._node_index],
            self._home_dir
        )
        self._processes["data_writer"] = start_data_writer(
            _cluster_name,
            self._node_name,
            _data_writer_addresses[self._node_index],
            _event_publisher_pull_addresses[self._node_index],
            self._home_dir
        )
        self._processes["handoff_server"] = start_handoff_server(
            _cluster_name,
            self._node_name,
            _handoff_server_addresses,
            _handoff_server_pipeline_addresses[self._node_index],
            _data_reader_addresses,
            _data_writer_addresses,
            self._home_dir
        )
        self._processes["anti_entropy_server"] = start_anti_entropy_server(
            _node_names,
            self._node_name,
            _anti_entropy_server_addresses,
            _anti_entropy_server_pipeline_addresses[self._node_index]
        )

        if self._space_accounting:
            self._processes["space_accounting"] = \
                start_space_accounting_server(
                    self._node_name,
                    _space_accounting_server_address,
                    _space_accounting_pipeline_address
                )

        if self._performance_packager:
            self._processes["performance-packager"] = \
                start_performance_packager(
                    self._node_name,
                    _event_publisher_pub_addresses
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

