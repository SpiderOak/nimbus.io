import subprocess
import random
import os
from base64 import b64encode
import atexit

_RUNNING_DATABASES = set()
_AT_EXIT_REGISTERED = False

def _stop_running_databases(running):
    "exit handler to stop any database instances that are running"
    for data_dir in sorted(running):
        print "Stopping database in %s" % ( data_dir, )
        _stop_running_database(data_dir)

def _stop_running_database(data_dir):
    cmd = ["pg_ctl", "stop", "-D", data_dir, ]
    print cmd
    code, out, err = run_cmd(cmd)

def create_database(cluster_config):
    """
    """

    database_users = dict(
        nimbusio_central_user = generate_db_user_pw(),
        nimbusio_node_user = generate_db_user_pw(),
    )

    for idx, name in enumerate(cluster_config.node_names):
        init_database_for_one_node(cluster_config, idx)
        start_database_for_one_node(cluster_config, idx)


def generate_db_user_pw():
    length = random.choice(range(8, 12))
    binary = os.urandom(length)
    return b64encode(binary)

def run_cmd(cmd, BUF_SIZE=-1):
    "run cmd and return ( exit code, stdout, stderr, )"
    program = subprocess.Popen(cmd, 
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=BUF_SIZE)
    out, err = program.communicate()
    code = program.returncode
    return code, out, err

def init_database_for_one_node(cluster_config, node_index):
    data_dir = cluster_config.db_data_paths[node_index]
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)

    cmd = ["pg_ctl", "initdb", "-D", data_dir, ] 
    print cmd
    code, out, err = run_cmd(cmd)
    print err, out
    if code != 0:
        raise RuntimeError("could not initdb cmd %r exit code %d: %s: %s" % (
            cmd, code, out, err, ))

def start_database_for_one_node(cluster_config, node_index):
    data_dir = cluster_config.db_data_paths[node_index]
    log_name = "postgresql-%s.log" % ( cluster_config.node_names[node_index], )
    log_path = os.path.join(cluster_config.log_path, log_name)
    # 2mb buffers, 50 connections, no fsync, port and socket dirs
    # (not much point in fsync with a simulator running manny databases on
    # one disk. as an added bonus, it may also help create corruption so we
    # can stress anti entropy and subtle failure modes)
    options = "-B 2MB -N 50 -F -p %d -k %s" % (
        cluster_config.db_ports[node_index], data_dir, )
    cmd = ["pg_ctl", "start", "-D", data_dir, "-l", log_path, "-o", options]
    print cmd
    code, out, err = run_cmd(cmd)
    print err, out
    if code != 0:
        raise RuntimeError("could not start db cmd %r exit code %d: %s: %s" % (
            cmd, code, out, err, ))

    _RUNNING_DATABASES.add(data_dir)

    # register our exit handler if we haven't already
    # it's annoying to have to manualy shut down a bunch of db instances
    if not _AT_EXIT_REGISTERED:
        global _AT_EXIT_REGISTERED
        _AT_EXIT_REGISTERED = True
        atexit.register(_stop_running_databases, _RUNNING_DATABASES)

def stop_database_for_one_node(cluster_config, node_index):
    data_dir = cluster_config.db_data_paths[node_index]
    cmd = ["pg_ctl", "stop", "-D", data_dir, ]
    print cmd
    code, out, err = run_cmd(cmd)
    print err, out
    if code != 0:
        raise RuntimeError("could not stop db cmd %r exit code %d: %s: %s" % (
            cmd, code, out, err, ))

    _RUNNING_DATABASES.discard(data_dir)


def populate_central_database(host, port, username, password):
    # create the user & pw, database, and schema
    pass 

def populate_node_database(host, port, username, password):
    # create the user & pw, database, and schema
    pass 

