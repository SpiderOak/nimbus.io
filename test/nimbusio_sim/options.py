import argparse

def parse_cmdline():
    """
    """

    parser = argparse.ArgumentParser(description='Run Simulated Cluster')

    parser.add_argument("--basedir", dest='basedir', action='store',
        default=None, 
        help="base directory to contain the simulated cluster")

    parser.add_argument("--baseport", dest="baseport", action="store",
        default=8000, type=int, 
        help="base network port to use")

    parser.add_argument("--clustername", dest="clustername", 
        action="store", default="multi-node-cluster", 
        help="name of the simulated cluster")

    parser.add_argument("--nodenamepattern", dest="nodenamepattern", 
        action="store", default="multi-node-%02d", 
        help="string format for generating node names")

    parser.add_argument("--nodecount", dest="nodecount", action="store", 
        default=10, help="node count")

    parser.add_argument("--systemdb", dest='systemdb', action='store_true',
        default=False, 
        help="use the system's DB instance (default: create new "
            "instances inside basedir)")

    parser.add_argument("--dbhost", dest="dbhost", action="store", 
        default="localhost", 
        help="database hostname (default=localhost), use w/ --systemdb")

    parser.add_argument("--singledb", dest="singledb", action="store_true", 
        default=False, 
        help="use a single instance of postgresql for central and node DBs")

    parser.add_argument("--ip", dest="ip", 
        action="store", default="127.0.0.1", 
        help="IP address to bind to (default: localhost)")

    parser.add_argument("--createnew", dest='createnew', action='store_true',
        default=False, 
        help="create a new cluster in basedir (default: use existing)")

    parser.add_argument("--logprune", dest="logprune", action="store_true",
        default=False, help="remove all existing logs")
    
    parser.add_argument("--start", dest="start", action="store_true",
        default=False, help="automatically start all nodes and web server")

    args = parser.parse_args()
    return args
