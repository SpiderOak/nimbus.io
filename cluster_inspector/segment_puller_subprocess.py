# -*- coding: utf-8 -*-
"""
segment_puller_subprocess.py
  
a subprocess runby cluster_inspector to pull segment data from individual
node databases
"""
import logging
import os
import sys

from tools.standard_logging import initialize_logging 
from tools.database_connection import get_node_connection

_local_node_name = os.environ["NIMBUSIO_NODE_NAME"]
_node_names = os.environ["NIMBUSIO_NODE_NAME_SEQ"].split()
_node_database_hosts = \
    os.environ["NIMBUSIO_NODE_DATABASE_HOSTS"].split()
_node_database_ports = \
    os.environ["NIMBUSIO_NODE_DATABASE_PORTS"].split()
_node_database_passwords = \
    os.environ["NIMBUSIO_NODE_USER_PASSWORDS"].split() 

def _pull_segment_data(connection, work_dir):
    pass

def main():
    """
    main entry point
    """
    [work_dir, index_str, ] = sys.argv[1:]
    index = int(index_str)
    node_name = _node_names[index]
    database_host = _node_database_hosts[index]
    database_port = _node_database_ports[index]
    database_password = _node_database_passwords[index]

    log_path = "{0}/nimbusio_segment_puller_from_{1}_to_{2}.log".format(
        os.environ["NIMBUSIO_LOG_DIR"], node_name, _local_node_name)
    initialize_logging(log_path)
    log = logging.getLogger("main")

    log.info("program starts")

    try:
        connection = get_node_connection(node_name,
                                         database_password,
                                         database_host,
                                         database_port)
    except Exception, instance:
        log.exception("Unable to connect to database {0}".format(instance))
        return -1

    try:
        _pull_segment_data(connection, work_dir)
    except Exception, instance:
        log.exception("_pull_segment_data failed {0}".format(instance))
        return -2
    finally:
        connection.close()

    log.info("program terminates normally")
    return 0
    
if __name__ == "__main__":
    sys.exit(main())
