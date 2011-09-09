# -*- coding: utf-8 -*-
"""
util.py

utility functions for unit tests
"""
import logging
import os
import os.path
import subprocess
import sys

def generate_key():
    """generate a unique key for data storage"""
    n = 0
    while True:
        n += 1
        yield "test-key-%06d" % (n, )

def identify_program_dir(target_dir):
    python_path = os.environ["PYTHONPATH"]
    for work_path in python_path.split(os.pathsep):
        test_path = os.path.join(work_path, target_dir)
        if os.path.isdir(test_path):
            return test_path

    raise ValueError(
        "Can't find %s in PYTHONPATH '%s'" % (target_dir, python_path, )
    )

def poll_process(process):
    process.poll()
    if process.returncode is None:
        return None

    return (process.returncode, process.stderr.read(), )

def terminate_process(process):
    process.terminate()
    process.wait()
    if process.returncode != 0:
        print >> sys.stderr, " "
        print >> sys.stderr, "-" * 15
        print >> sys.stderr, process.returncode
        print >> sys.stderr, process.stderr.read()
        print >> sys.stderr, "-" * 15
        print >> sys.stderr, process.returncode
    assert process.returncode == 0, \
        process.returncode

def start_data_writer(
    cluster_name, 
    node_name, 
    address, 
    event_publisher_pull_address, 
    repository_path
):
    log = logging.getLogger("start_data_writer_%s" % (node_name, ))
    server_dir = identify_program_dir(u"data_writer")
    server_path = os.path.join(server_dir, "data_writer_main.py")
    
    args = [
        sys.executable,
        server_path,
    ]

    environment = {
        "PYTHONPATH"                        : os.environ["PYTHONPATH"],
        "NIMBUSIO_LOG_DIR"         : os.environ["NIMBUSIO_LOG_DIR"],
        "NIMBUSIO_CLUSTER_NAME"      : cluster_name,
        "NIMBUSIO_NODE_NAME"         : node_name,
        "NIMBUSIO_DATA_WRITER_ADDRESS"        : address,
        "NIMBUSIO_REPOSITORY_PATH"            : repository_path,
        "NIMBUSIO_EVENT_PUBLISHER_PULL_ADDRESS" : \
            event_publisher_pull_address,
        "NIMBUSIO_CENTRAL_USER_PASSWORD"          : "pork",
        "NIMBUSIO_NODE_USER_PASSWORD"             : "pork",
    }        

    log.info("starting %s %s" % (args, environment, ))
    return subprocess.Popen(args, stderr=subprocess.PIPE, env=environment)

def start_data_reader(
    node_name, 
    address, 
    event_publisher_pull_address, 
    repository_path
):
    log = logging.getLogger("start_data_reader_%s" % (node_name, ))
    server_dir = identify_program_dir(u"data_reader")
    server_path = os.path.join(server_dir, "data_reader_main.py")
    
    args = [
        sys.executable,
        server_path,
    ]

    environment = {
        "PYTHONPATH"                        : os.environ["PYTHONPATH"],
        "NIMBUSIO_LOG_DIR"         : os.environ["NIMBUSIO_LOG_DIR"],
        "NIMBUSIO_NODE_NAME"         : node_name,
        "NIMBUSIO_DATA_READER_ADDRESS"        : address,
        "NIMBUSIO_REPOSITORY_PATH"            : repository_path,
        "NIMBUSIO_EVENT_PUBLISHER_PULL_ADDRESS" : \
            event_publisher_pull_address,
        "NIMBUSIO_NODE_USER_PASSWORD"             : "pork",
    }        

    log.info("starting %s %s" % (args, environment, ))
    return subprocess.Popen(args, stderr=subprocess.PIPE, env=environment)

def start_anti_entropy_server(
    cluster_name,
    node_names,
    node_name, 
    anti_entropy_server_addresses,
    pipeline_address,
    event_publisher_pull_address, 
):
    log = logging.getLogger("_start_anti_entropy_server%s" % (node_name, ))
    server_dir = identify_program_dir(u"anti_entropy_server")
    server_path = os.path.join(
        server_dir, "anti_entropy_server_main.py"
    )
    
    args = [
        sys.executable,
        server_path,
    ]

    environment = {
        "PYTHONPATH"                        : os.environ["PYTHONPATH"],
        "NIMBUSIO_LOG_DIR"         : os.environ["NIMBUSIO_LOG_DIR"],
        "NIMBUSIO_CLUSTER_NAME"      : cluster_name,
        "NIMBUSIO_NODE_NAME_SEQ"     : \
            " ".join(node_names),
        "NIMBUSIO_NODE_NAME"         : node_name,
        "NIMBUSIO_ANTI_ENTROPY_SERVER_ADDRESSES": \
            " ".join(anti_entropy_server_addresses),
        "NIMBUSIO_ANTI_ENTROPY_SERVER_PIPELINE_ADDRESS": pipeline_address,
        "NIMBUSIO_CENTRAL_USER_PASSWORD"             : "pork",
        "NIMBUSIO_NODE_USER_PASSWORD"             : "pork",
        "NIMBUSIO_EVENT_PUBLISHER_PULL_ADDRESS" : \
            event_publisher_pull_address,
    }        

    log.info("starting %s %s" % (args, environment, ))
    return subprocess.Popen(args, stderr=subprocess.PIPE, env=environment)

def start_space_accounting_server(
    node_name, 
    address, 
    pipeline_address,
    event_publisher_pull_address, 
):
    log = logging.getLogger("_start_space_accounting_server%s" % (node_name, ))
    server_dir = identify_program_dir(u"space_accounting_server")
    server_path = os.path.join(
        server_dir, "space_accounting_server_main.py"
    )
    
    args = [
        sys.executable,
        server_path,
    ]

    environment = {
        "PYTHONPATH"                        : os.environ["PYTHONPATH"],
        "NIMBUSIO_LOG_DIR"         : os.environ["NIMBUSIO_LOG_DIR"],
        "NIMBUSIO_NODE_NAME"         : node_name,
        "NIMBUSIO_SPACE_ACCOUNTING_SERVER_ADDRESS" : address,
        "NIMBUSIO_SPACE_ACCOUNTING_PIPELINE_ADDRESS" : pipeline_address,
        "NIMBUSIO_CENTRAL_USER_PASSWORD"             : "pork",
        "NIMBUSIO_EVENT_PUBLISHER_PULL_ADDRESS" : \
            event_publisher_pull_address,
    }        

    log.info("starting %s %s" % (args, environment, ))
    return subprocess.Popen(args, stderr=subprocess.PIPE, env=environment)

def start_handoff_server(
    cluster_name,
    local_node_name, 
    handoff_server_addresses, 
    pipeline_address, 
    data_reader_addresses, 
    data_writer_addresses, 
    event_publisher_pull_address, 
    repository_path
):
    log = logging.getLogger("start_handoff_server_%s" % (local_node_name, ))
    server_dir = identify_program_dir(u"handoff_server")
    server_path = os.path.join(server_dir, "handoff_server_main.py")
    
    args = [
        sys.executable,
        server_path,
    ]

    environment = {
        "PYTHONPATH"                        : os.environ["PYTHONPATH"],
        "NIMBUSIO_LOG_DIR"         : os.environ["NIMBUSIO_LOG_DIR"],
        "NIMBUSIO_CLUSTER_NAME"      : cluster_name, 
        "NIMBUSIO_NODE_NAME"         : local_node_name,
        "NIMBUSIO_HANDOFF_SERVER_ADDRESSES"        : " ".join(
            handoff_server_addresses
        ),
        "NIMBUSIO_HANDOFF_SERVER_PIPELINE_ADDRESS": pipeline_address,
        "NIMBUSIO_DATA_READER_ADDRESSES"    : " ".join(data_reader_addresses),
        "NIMBUSIO_DATA_WRITER_ADDRESSES"    : " ".join(data_writer_addresses),
        "NIMBUSIO_EVENT_PUBLISHER_PULL_ADDRESS" : \
            event_publisher_pull_address,
        "NIMBUSIO_REPOSITORY_PATH"            : repository_path,
        "NIMBUSIO_CENTRAL_USER_PASSWORD"             : "pork",
        "NIMBUSIO_NODE_USER_PASSWORD"             : "pork",
    }        

    log.info("starting %s %s" % (args, environment, ))
    return subprocess.Popen(args, stderr=subprocess.PIPE, env=environment)

def start_event_publisher(node_name, pull_address, pub_address):
    log = logging.getLogger("start_event_publisher_%s" % (node_name, ))
    server_dir = identify_program_dir(u"event_publisher")
    server_path = os.path.join(server_dir, "event_publisher_main.py")
    
    args = [
        sys.executable,
        server_path,
    ]

    environment = {
        "PYTHONPATH"                            : os.environ["PYTHONPATH"],
        "NIMBUSIO_LOG_DIR"         : os.environ["NIMBUSIO_LOG_DIR"],
        "NIMBUSIO_NODE_NAME"             : node_name,
        "NIMBUSIO_EVENT_PUBLISHER_PULL_ADDRESS"   : pull_address,
        "NIMBUSIO_EVENT_PUBLISHER_PUB_ADDRESS"    : pub_address,
    }        

    log.info("starting %s %s" % (args, environment, ))
    return subprocess.Popen(args, stderr=subprocess.PIPE, env=environment)

def start_performance_packager(node_name, pub_addresses):
    log = logging.getLogger("start_performance_packager_%s" % (node_name, ))
    server_dir = identify_program_dir(u"performance_packager")
    server_path = os.path.join(
        server_dir, "performance_packager_main.py"
    )
    
    args = [
        sys.executable,
        server_path,
    ]

    environment = {
        "PYTHONPATH"                            : os.environ["PYTHONPATH"],
        "NIMBUSIO_LOG_DIR"         : os.environ["NIMBUSIO_LOG_DIR"],
        "NIMBUSIO_NODE_NAME"             : node_name,
        "NIMBUSIO_EVENT_PUBLISHER_PUB_ADDRESSES"  : " ".join(pub_addresses),
    }        

    log.info("starting %s %s" % (args, environment, ))
    return subprocess.Popen(args, stderr=subprocess.PIPE, env=environment)

