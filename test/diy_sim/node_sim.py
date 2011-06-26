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
        start_space_accounting_server, \
        start_anti_entropy_server, \
        start_handoff_server, \
        poll_process, \
        terminate_process

def _generate_node_name(node_index):
    return "node-sim-%02d" % (node_index, )

_node_count = 10
_data_writer_base_port = 8100
_data_reader_base_port = 8300
_handoff_server_base_port = 8700
_data_writer_addresses = [
    "tcp://127.0.0.1:%s" % (_data_writer_base_port+i, ) \
    for i in range(_node_count)
]
_data_writer_pipeline_addresses = [
    "ipc:///tmp/spideroak-diyapi-data-writer-pipeline-%s/socket" % (
        _generate_node_name( i ),
    ) \
    for i in range(_node_count)
]
_data_reader_addresses = [
    "tcp://127.0.0.1:%s" % (_data_reader_base_port+i, ) \
    for i in range(_node_count)
]
_data_reader_pipeline_addresses = [
    "ipc:///tmp/spideroak-diyapi-data-reader-pipeline-%s/socket" % (
        _generate_node_name( i ),
    ) \
    for i in range(_node_count)
]
_space_accounting_server_address = "tcp://127.0.0.1:8500"
_space_accounting_pipeline_address = "tcp://127.0.0.1:8550"
_anti_entropy_server_address = "tcp://127.0.0.1:8600"
_anti_entropy_server_pipeline_address = "tcp://127.0.0.1:8650"
_handoff_server_addresses = [
    "ipc:///tmp/spideroak-diyapi-handoff_server-%s/socket" % (
        _generate_node_name( i ),
    ) \
    for i in range(_node_count)
]
_handoff_server_pipeline_addresses = [
    "tcp://127.0.0.1:%s" % (_handoff_server_base_port+i, ) \
    for i in range(_node_count)
]
_node_names = [_generate_node_name(i) for i in range(_node_count)]

class NodeSim(object):
    """simulate one node in a cluster"""

    def __init__(
        self, test_dir, node_index, space_accounting=False, anti_entropy=False
    ):
        self._node_index = node_index
        self._node_name = _generate_node_name(node_index)
        self._log = logging.getLogger(self._node_name)
        self._home_dir = os.path.join(test_dir, self._node_name)
        if not os.path.exists(self._home_dir):
            os.makedirs(self._home_dir)
        self._processes = dict()
        self._space_accounting = space_accounting
        self._anti_entropy = anti_entropy

    def __str__(self):
        return self._node_name

    def start(self):
        self._log.debug("start")

        self._processes["data_writer"] = start_data_writer(
            self._node_name,
            _data_writer_addresses[self._node_index],
            _data_writer_pipeline_addresses[self._node_index],
            self._home_dir
        )
        self._processes["handoff_server"] = start_handoff_server(
            _node_names,
            self._node_name,
            _handoff_server_addresses[self._node_index],
            _handoff_server_pipeline_addresses[self._node_index],
            _data_reader_addresses,
            _data_writer_addresses,
            self._home_dir
        )

        if self._space_accounting:
            self._processes["space_accounting"] = \
                start_space_accounting_server(
                    self._node_name,
                    _space_accounting_server_address,
                    _space_accounting_pipeline_address
                )

        if self._anti_entropy:
            self._processes["anti_entropy"] = start_anti_entropy_server(
                _node_names,
                self._node_name,
                _anti_entropy_server_address,
                _anti_entropy_server_pipeline_address,
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

