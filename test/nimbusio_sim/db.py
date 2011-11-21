import time
import subprocess
import random
import os
from base64 import b64encode
import atexit
import pwd
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT, \
    ISOLATION_LEVEL_READ_COMMITTED

_RUNNING_DATABASES = set()
_AT_EXIT_REGISTERED = False
_DB_HOST = "localhost"

# exit handler system to shutdown databases left running if the simulator
# crashes.
# it's annoying to have to manualy shut down a bunch of db instances

def _stop_running_database(data_dir):
    "stop the database running in data_dir"
    cmd = ["pg_ctl", "stop", "-D", data_dir, ]
    print cmd
    code, out, err = run_cmd(cmd)

def _stop_running_databases(running):
    "exit handler to stop any database instances that are running"
    for data_dir in sorted(running):
        print "Stopping database in %s" % ( data_dir, )
        _stop_running_database(data_dir)

if not _AT_EXIT_REGISTERED:
    _AT_EXIT_REGISTERED = True
    atexit.register(_stop_running_databases, _RUNNING_DATABASES)

def find_schema_path(schema_filename):
    base_path = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    sql_path = os.path.join(base_path, "sql")
    schema_path = os.path.join(sql_path, schema_filename)
    assert os.path.exists(schema_path), schema_path
    return schema_path

def create_database(cluster_config):
    """
    """

    # generate passwords for all the db users
    database_users = dict()
    database_users[cluster_config.central_db_user] = generate_db_user_pw()
    for name in cluster_config.node_db_users:
        database_users[name] = generate_db_user_pw()

    # this definitely won't be portable to Windows...
    superuser_name = pwd.getpwuid(os.getuid())[0]
    
    # create and start central db
    init_db(cluster_config.central_db_path)
    start_db(cluster_config.central_db_path, 
             cluster_config.central_db_port, 
             os.path.join(cluster_config.log_path, "postgresql-central.log"))

    create_owner_and_database(_DB_HOST,
                              cluster_config.central_db_port,
                              superuser_name, 
                              cluster_config.central_db_name,
                              cluster_config.central_db_user, 
                              database_users[cluster_config.central_db_user])

    apply_database_schema(find_schema_path("nimbusio_central.sql"),
        _DB_HOST, cluster_config.central_db_port,
        cluster_config.central_db_name, cluster_config.central_db_user)
    
    # create and start all the node DBs
    for idx, name in enumerate(cluster_config.node_names):
        init_db(cluster_config.node_db_paths[idx])
        start_db(cluster_config.node_db_paths[idx],
                 cluster_config.node_db_ports[idx],
                 os.path.join(cluster_config.log_path,
                    "postgresql-node-%s.log" % ( name, )))
        create_owner_and_database(
            _DB_HOST, 
            cluster_config.node_db_ports[idx],
            superuser_name,
            cluster_config.node_db_names[idx],
            cluster_config.node_db_users[idx],
            database_users[cluster_config.node_db_users[idx]])
        apply_database_schema(
            find_schema_path("nimbusio_node.sql"),
            _DB_HOST, 
            cluster_config.node_db_ports[idx],
            cluster_config.node_db_names[idx],
            cluster_config.node_db_users[idx])

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
    init_db(cluster_config.node_db_paths[node_index])

def init_central_database(cluster_config):
    init_db(cluster_config.central_db_path)

def init_db(data_dir):
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
    cmd = ["pg_ctl", "initdb", "-D", data_dir, ] 
    print cmd
    code, out, err = run_cmd(cmd)
    print err, out
    if code != 0:
        raise RuntimeError("could not initdb cmd %r exit code %d: %s: %s" % (
            cmd, code, out, err, ))

def start_db(data_dir, port, log_path):
    options = "-B 2MB -N 50 -F -p %d -k %s" % (
        port, data_dir, )
    cmd = ["pg_ctl", "start", "-D", data_dir, "-l", log_path, "-o", options]
    print cmd
    code, out, err = run_cmd(cmd)
    print err, out
    if code != 0:
        raise RuntimeError("could not start db cmd %r exit code %d: %s: %s" % (
            cmd, code, out, err, ))

    _RUNNING_DATABASES.add(data_dir)

def start_database_for_one_node(cluster_config, node_index):
    data_dir = cluster_config.node_db_paths[node_index]
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


def stop_database_for_one_node(cluster_config, node_index):
    data_dir = cluster_config.node_db_paths[node_index]
    cmd = ["pg_ctl", "stop", "-D", data_dir, ]
    print cmd
    code, out, err = run_cmd(cmd)
    print err, out
    if code != 0:
        raise RuntimeError("could not stop db cmd %r exit code %d: %s: %s" % (
            cmd, code, out, err, ))

    _RUNNING_DATABASES.discard(data_dir)


def retry_db_connect(params, max_retry=20):
    print repr(params)
    counter = 0
    while True:
        try:
            conn = psycopg2.connect(**params)
            break
        except Exception, err:
            counter += 1
            if counter > max_retry:
                raise
            time.sleep(1)

    return conn

def create_owner_and_database(host, port, superuser_name, 
                              new_db_name, owner_username, owner_password):

    params = dict(database="template1", user=superuser_name, 
                  host=host, port=port)
    conn = retry_db_connect(params)
    sql = 'create role "%s" with login password %%s' % (owner_username, )
    print sql
    cursor = conn.cursor()
    cursor.execute(sql, [owner_password])
    conn.commit()
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    sql = 'create database "%s" with owner "%s"' % ( 
        new_db_name, owner_username, )
    print sql
    cursor.execute(sql, [])
    conn.set_isolation_level(ISOLATION_LEVEL_READ_COMMITTED)
    conn.close()

def apply_database_schema(schemapath, host, port, dbname, db_username):
    if not os.path.exists(schemapath):
        raise RuntimeError("schema %s not found" % (schemapath, ))

    cmd = ["bash", "-c", 
           "psql -h %s -p %d -U %s -d %s < %s" % ( host, port, db_username, 
                                                   dbname, schemapath, ) ]
    code, out, err = run_cmd(cmd)
    if code != 0:
        raise RuntimeError("cmd failed: %r exit code %d: %s: %s" % (
            cmd, code, out, err, ))
    return True

#def populate_central_database(host, port, superuser_name, new_db_name, username, password):
#    # create the user & pw, database, and schema

def populate_node_database(host, port, superuser_name, username, password):
    # create the user & pw, database, and schema
    pass 

