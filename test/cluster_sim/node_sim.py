# -*- coding: utf-8 -*-
"""
node_sim.py

simulate one node in a cluster
"""
import logging
import os
import os.path
import subprocess

_rabbitmq_base_port = 6000
_rabbitmq_ip_address = "127.0.0.1"
_rabbitmq_bin = os.environ.get("RABBITMQ_BIN", "/usr/sbin")
_rabbitmq_server = os.path.join(_rabbitmq_bin, "rabbitmq-server")
_rabbitmq_ctl = os.path.join(_rabbitmq_bin, "rabbitmqctl")

class NodeSim(object):
    """simulate one node in a cluster"""
    def __init__(self, node_index):
        self._node_index = node_index
        self._node_name = "node-sim-%02d" % (node_index, )
        self._rabbitmq_port = _rabbitmq_base_port + self._node_index 
        self._log = logging.getLogger(self._node_name)

    def start(self):
        self._log.debug("start")
        subprocess.check_call(
            [_rabbitmq_server, "-detached", ], 
            env=self._rabbitmq_env()
        )

    def stop(self):
        self._log.debug("stop")
        subprocess.check_call(
            [_rabbitmq_ctl, "stop", ], 
            env=self._rabbitmq_env()
        )

    def _rabbitmq_env(self):
        return {
            "HOME"                        : os.environ["HOME"],
            "PATH"                        : os.environ["PATH"],
            "RABBITMQ_NODENAME"           : self._node_name, 
            "RABBITMQ_NODE_IP_ADDRESS"    : _rabbitmq_ip_address,
            "RABBITMQ_NODE_PORT"          : str(self._rabbitmq_port),
        }
        #"RABBITMQ_CLUSTER_CONFIG_FILE" 
        #"RABBITMQ_CONFIG_FILE"

