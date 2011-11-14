"""
"""

class ClusterConfig(object):
    """
    Hold configuration state for cluster
    """
    def __init__(self, args):
        self.config = args
        self._writer_base_port       = args.baseport
        self._reader_base_port       = args.nodecount + args.baseport
        self._anti_entropy_base_port = 2 * args.nodecount + args.baseport
        self._handoff_base_port      = 3 * args.nodecount + args.baseport
        self._handoff_pipe_base_port = 4 * args.nodecount + args.baseport

    def __getattr__(self, name):
        if not hasattr(self.config, name):
            raise AttributeError(name)
        return getattr(self.config, name)

    def data_writer_addresses(self):
        return [ "tcp://127.0.0.1:%s" % i
            for i in range(self._writer_base_port, 
                           self._writer_base_port + self.nodecount)]

    def data_reader_addresses(self):
        return [ "tcp://127.0.0.1:%s" % i
            for i in range(self._reader_base_port, 
                           self._reader_base_port + self.nodecount)]

    def data_reader_addresses(self):
        return [ "tcp://127.0.0.1:%s" % i
            for i in range(self._reader_base_port, 
                           self._reader_base_port + self.nodecount)]
