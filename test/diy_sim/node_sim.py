# -*- coding: utf-8 -*-
"""
node_sim.py

simulate one node in a cluster
"""
import logging
import os
import os.path
import signal
import subprocess
import sys
import time

class SimError(Exception):
    pass

_rabbitmq_base_port = 6000
_rabbitmq_ip_address = "127.0.0.1"

def _identify_program_dir(target_path):
    python_path = os.environ["PYTHONPATH"]
    for work_path in python_path.split(os.pathsep):
        test_path = os.path.join(work_path, target_path)
        if os.path.isdir(test_path):
            return test_path

    raise SimError(
        "Can't find %s in PYTHONPATH '%s'" % (target_path, python_path, )
    )
                
class NodeSim(object):
    """simulate one node in a cluster"""
    def __init__(self, test_dir, node_index):
        self._node_index = node_index
        self._node_name = "node-sim-%02d" % (node_index, )
        self._rabbitmq_port = _rabbitmq_base_port + self._node_index 
        self._log = logging.getLogger(self._node_name)
        self._home_dir = os.path.join(test_dir, self._node_name)
        if not os.path.exists(self._home_dir):
            os.makedirs(self._home_dir)
        self._processes = dict()

    def start(self):
        self._log.debug("start")

        process_specs = [
            ("database_server",     self._start_database_server, ),
            ("data_writer",         self._start_data_writer, ),
            ("data_reader",         self._start_data_reader, ),
            ("handoff_server",      self._start_handoff_server, ),
        ]

        for process_name, start_function in process_specs:
            self._processes[process_name] = start_function()
            time.sleep(1.0)

    def stop(self):
        self._log.debug("stop")
        for process_name in self._processes.keys():
            self._stop_process(process_name)

    def poll(self):
        """check the condition of running processes"""
        self._log.debug("polling")

        for process_name in self._processes.keys():
            process = self._processes[process_name]
            if process is not None:
                process.poll()
                if process.returncode is not None:
                    error_string = "%s terminated abnormally (%s) '%s'" % (
                        process_name,
                        process.returncode, 
                        process.stderr.read(),
                    )
                    print error_string
                    self._log.error(error_string)
                    self._processes[process_name] = None

    def _stop_process(self, process_name):
        self._log.info("stopping process %s" % (process_name, ))
        process = self._processes[process_name]
        if process is not None:
            os.kill(process.pid, signal.SIGTERM)
            process.wait()
            self._log.debug("process %s returncode = '%s' %s" % (
                process_name, process.returncode, process.stderr.read(),
            ))
            self._processes[process_name] = None
        else:
            self._log.warn("Attempt to close nonexistent process %s" % (
                process_name,
            ))

    def _start_database_server(self):
        database_server_dir = _identify_program_dir(u"diyapi_database_server")
        database_server_path = os.path.join(
            database_server_dir, u"diyapi_database_server_main.py"
        )
        
        args = [sys.executable, database_server_path, ]

        environment = self._rabbitmq_env()
        environment["PYTHONPATH"] = os.environ["PYTHONPATH"]
        environment["SPIDEROAK_MULTI_NODE_NAME"] = self._node_name
        environment["DIYAPI_REPOSITORY_PATH"] = self._home_dir

        self._log.info("starting database_server")

        return subprocess.Popen(args, stderr=subprocess.PIPE, env=environment)

    def _start_data_writer(self):
        data_writer_dir = _identify_program_dir(u"diyapi_data_writer")
        data_writer_path = os.path.join(
            data_writer_dir, u"diyapi_data_writer_main.py"
        )
        
        args = [sys.executable, data_writer_path, ]

        environment = self._rabbitmq_env()
        environment["PYTHONPATH"] = os.environ["PYTHONPATH"]
        environment["SPIDEROAK_MULTI_NODE_NAME"] = self._node_name
        environment["DIYAPI_REPOSITORY_PATH"] = self._home_dir

        self._log.info("starting data_writer")

        return subprocess.Popen(args, stderr=subprocess.PIPE, env=environment)

    def _start_data_reader(self):
        data_reader_dir = _identify_program_dir(u"diyapi_data_reader")
        data_reader_path = os.path.join(
            data_reader_dir, u"diyapi_data_reader_main.py"
        )
        
        args = [sys.executable, data_reader_path, ]

        environment = self._rabbitmq_env()
        environment["PYTHONPATH"] = os.environ["PYTHONPATH"]
        environment["SPIDEROAK_MULTI_NODE_NAME"] = self._node_name
        environment["DIYAPI_REPOSITORY_PATH"] = self._home_dir

        self._log.info("starting data_reader")

        return subprocess.Popen(args, stderr=subprocess.PIPE, env=environment)

    def _start_handoff_server(self):
        handoff_server_dir = _identify_program_dir(u"diyapi_handoff_server")
        handoff_server_path = os.path.join(
            handoff_server_dir, u"diyapi_handoff_server_main.py"
        )
        
        args = [sys.executable, handoff_server_path, ]

        environment = self._rabbitmq_env()
        environment["PYTHONPATH"] = os.environ["PYTHONPATH"]
        environment["SPIDEROAK_MULTI_NODE_NAME"] = self._node_name
        environment["DIYAPI_REPOSITORY_PATH"] = self._home_dir

        self._log.info("starting handoff_server")

        return subprocess.Popen(args, stderr=subprocess.PIPE, env=environment)

    def _rabbitmq_env(self):
        return {
            "HOME"                        : os.environ["HOME"],
            "RABBITMQ_NODENAME"           : self._node_name, 
            "RABBITMQ_NODE_IP_ADDRESS"    : _rabbitmq_ip_address,
            "RABBITMQ_NODE_PORT"          : str(self._rabbitmq_port),
        }

