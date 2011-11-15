"""
"""

class ClusterConfig(object):
    """
    Hold configuration state for cluster
    """
    def __init__(self, args):
        self.config = args
        self.base_ports = dict(
            writer =            args.baseport,
            reader =            1 * args.nodecount + args.baseport,
            anti_entropy =      2 * args.nodecount + args.baseport, 
            handoff =           3 * args.nodecount + args.baseport, 
            handoff_pipeline =  4 * args.nodecount + args.baseport )

    def __getattr__(self, name):
        if not hasattr(self.config, name):
            raise AttributeError(name)
        return getattr(self.config, name)

    @property
    def node_names(self):
        return [self.nodenamepattern % i for i in range(1, self.nodecount + 1)]

    def _node_service_addresses(name):
        return [ "tcp://127.0.0.1:%s" % i
            for i in range(self.base_ports['name']
                           self.base_ports['name'] + self.nodecount)]

    @property
    def space_accounting_server_address(self):
        return "tcp://127.0.0.1:%d" % ( max(self.base_ports.values()) 
                                        + self.nodecount, )

    @property
    def space_accounting_pipeline_address(self):
        return "tcp://127.0.0.1:%d" % ( max(self.base_ports.values()) 
                                        + self.nodecount + 1 )

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
    def handoff_server_addresses(self):
        return self._node_service_addresses('handoff')

    @property
    def handoff_server_pipeline_addresses(self):
        return self._node_service_addresses('handoff_pipeline')
