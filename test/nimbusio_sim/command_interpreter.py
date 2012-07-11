# -*- coding: utf-8 -*-
"""
command_interpreter.py

accept commands while running simulated nodes
"""
import cmd
import time

from test.nimbusio_sim.node_sim import NodeSim
from test.nimbusio_sim.profile_graphs import prepare_profile_graphs

_node_count = 10

class CommandInterpreter(cmd.Cmd):
    """
    accept commands while running simulated nodes
    """
    def __init__(self, config, createnew):
        cmd.Cmd.__init__(self)
        self._cluster_config = config
        self._node_sims = list()
        self._node_sims.append(
            NodeSim(
                0, 
                config,
                createnew,
                space_accounting=True, 
                performance_packager=True,
                event_aggregator=True,
                event_subscriber=True,
                stats_subscriber=True,
                web_server=True,
                web_manager=True,
            )
        )
        for i in xrange(_node_count-1):
            self._node_sims.append(
                NodeSim(i+1, config, createnew, web_server=True))

    def _get_node_from_line(self, line):
        try:
            index = int(line) - 1
        except ValueError:
            index = len(self._node_sims) + 1
            
        try:
            return self._node_sims[index]
        except IndexError:
            print "Please enter an integer betwee 1 and 10"
            return None

    def do_start(self, line):
        """start a node (1..10) or 'all'"""
        if line in ["", "all"]:
            for node_sim in self._node_sims:
                print "starting", str(node_sim)
                node_sim.start()
                time.sleep(1.0)
            return
        
        node_sim = self._get_node_from_line(line)
        if node_sim is not None:
            print "starting", str(node_sim)
            node_sim.start()

    def do_stop_node(self, line):
        """stop one node"""
        node_sim = self._get_node_from_line(line)
        if node_sim is not None:
            print "stopping", str(node_sim)
            node_sim.stop()

    def do_poll(self, line):
        """poll nodes for subprocess status"""
        if line in ["", "all"]:
            for node_sim in self._node_sims:
                print "polling", str(node_sim)
                node_sim.poll()
            return

        node_sim = self._get_node_from_line(line)
        if node_sim is not None:
            print "polling", str(node_sim)
            node_sim.poll()

    def do_halt(self, _line):
        """stop the command interpreter and exit the program"""
        for node_sim in self._node_sims:
            print "stopping", str(node_sim)
            try:
                node_sim.stop()
            except Exception, instance:
                print "*** Exception", str(instance), "***"

        if self._cluster_config.profile:
            print "preparing profile graphs"
            prepare_profile_graphs(self._cluster_config.profile_path)

        return True

    def do_quit(self, _line):
        """stop the command interpreter and exit the program"""
        return self.onecmd("halt")
    def do_exit(self, _line):
        """stop the command interpreter and exit the program"""
        return self.onecmd("halt")

