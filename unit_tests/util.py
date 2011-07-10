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

def random_string(size):
    with open('/dev/urandom', 'rb') as f:
        return f.read(size)

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
    node_name, address, event_publisher_pull_address, repository_path
):
    log = logging.getLogger("start_data_writer_%s" % (node_name, ))
    server_dir = identify_program_dir(u"diyapi_data_writer")
    server_path = os.path.join(server_dir, "diyapi_data_writer_main.py")
    
    args = [
        sys.executable,
        server_path,
    ]

    environment = {
        "PYTHONPATH"                        : os.environ["PYTHONPATH"],
        "SPIDEROAK_MULTI_NODE_NAME"         : node_name,
        "DIYAPI_DATA_WRITER_ADDRESS"        : address,
        "DIYAPI_REPOSITORY_PATH"            : repository_path,
        "DIYAPI_EVENT_PUBLISHER_PULL_ADDRESS" : \
            event_publisher_pull_address,
        "PANDORA_DB_PW_pandora"             : "pork",
    }        

    log.info("starting %s %s" % (args, environment, ))
    return subprocess.Popen(args, stderr=subprocess.PIPE, env=environment)

def start_data_reader(node_name, address, repository_path):
    log = logging.getLogger("start_data_reader_%s" % (node_name, ))
    server_dir = identify_program_dir(u"diyapi_data_reader")
    server_path = os.path.join(server_dir, "diyapi_data_reader_main.py")
    
    args = [
        sys.executable,
        server_path,
    ]

    environment = {
        "PYTHONPATH"                        : os.environ["PYTHONPATH"],
        "SPIDEROAK_MULTI_NODE_NAME"         : node_name,
        "DIYAPI_DATA_READER_ADDRESS"        : address,
        "DIYAPI_REPOSITORY_PATH"            : repository_path,
        "PANDORA_DB_PW_pandora"             : "pork",
    }        

    log.info("starting %s %s" % (args, environment, ))
    return subprocess.Popen(args, stderr=subprocess.PIPE, env=environment)

def start_anti_entropy_server(
    node_names,
    node_name, 
    anti_entropy_server_addresses,
    pipeline_address,
):
    log = logging.getLogger("_start_anti_entropy_server%s" % (node_name, ))
    server_dir = identify_program_dir(u"diyapi_anti_entropy_server")
    server_path = os.path.join(
        server_dir, "diyapi_anti_entropy_server_main.py"
    )
    
    args = [
        sys.executable,
        server_path,
    ]

    environment = {
        "PYTHONPATH"                        : os.environ["PYTHONPATH"],
        "SPIDEROAK_MULTI_NODE_NAME_SEQ"     : \
            " ".join(node_names),
        "SPIDEROAK_MULTI_NODE_NAME"         : node_name,
        "DIYAPI_ANTI_ENTROPY_SERVER_ADDRESSES": \
            " ".join(anti_entropy_server_addresses),
        "DIYAPI_ANTI_ENTROPY_SERVER_PIPELINE_ADDRESS": pipeline_address,
        "PANDORA_DB_PW_pandora_storage_server" : \
            os.environ["PANDORA_DB_PW_pandora_storage_server"],
        "PANDORA_DB_PW_pandora" : os.environ["PANDORA_DB_PW_pandora"],
        "PANDORA_DB_PW_diyapi_auditor" : \
            os.environ["PANDORA_DB_PW_diyapi_auditor"]
    }        

    log.info("starting %s %s" % (args, environment, ))
    return subprocess.Popen(args, stderr=subprocess.PIPE, env=environment)

def start_space_accounting_server(node_name, address, pipeline_address):
    log = logging.getLogger("_start_space_accounting_server%s" % (node_name, ))
    server_dir = identify_program_dir(u"diyapi_space_accounting_server")
    server_path = os.path.join(
        server_dir, "diyapi_space_accounting_server_main.py"
    )
    
    args = [
        sys.executable,
        server_path,
    ]

    environment = {
        "PYTHONPATH"                        : os.environ["PYTHONPATH"],
        "SPIDEROAK_MULTI_NODE_NAME"         : node_name,
        "PANDORA_DB_PW_pandora_storage_server" : \
            os.environ["PANDORA_DB_PW_pandora_storage_server"],
        "DIYAPI_SPACE_ACCOUNTING_SERVER_ADDRESS" : address,
        "DIYAPI_SPACE_ACCOUNTING_PIPELINE_ADDRESS" : pipeline_address,
    }        

    log.info("starting %s %s" % (args, environment, ))
    return subprocess.Popen(args, stderr=subprocess.PIPE, env=environment)

def start_handoff_server(
    local_node_name, 
    handoff_server_addresses, 
    pipeline_address, 
    data_reader_addresses, 
    data_writer_addresses, 
    repository_path
):
    log = logging.getLogger("start_handoff_server_%s" % (local_node_name, ))
    server_dir = identify_program_dir(u"diyapi_handoff_server")
    server_path = os.path.join(server_dir, "diyapi_handoff_server_main.py")
    
    args = [
        sys.executable,
        server_path,
    ]

    environment = {
        "PYTHONPATH"                        : os.environ["PYTHONPATH"],
        "SPIDEROAK_MULTI_NODE_NAME"         : local_node_name,
        "DIYAPI_HANDOFF_SERVER_ADDRESSES"        : " ".join(
            handoff_server_addresses
        ),
        "DIYAPI_HANDOFF_SERVER_PIPELINE_ADDRESS": pipeline_address,
        "DIYAPI_DATA_READER_ADDRESSES"    : " ".join(data_reader_addresses),
        "DIYAPI_DATA_WRITER_ADDRESSES"    : " ".join(data_writer_addresses),
        "DIYAPI_REPOSITORY_PATH"            : repository_path,
        "PANDORA_DB_PW_pandora" :  os.environ["PANDORA_DB_PW_pandora"],
    }        

    log.info("starting %s %s" % (args, environment, ))
    return subprocess.Popen(args, stderr=subprocess.PIPE, env=environment)

def start_event_publisher(node_name, pull_address, pub_address):
    log = logging.getLogger("start_event_publisher_%s" % (node_name, ))
    server_dir = identify_program_dir(u"diyapi_event_publisher")
    server_path = os.path.join(server_dir, "diyapi_event_publisher_main.py")
    
    args = [
        sys.executable,
        server_path,
    ]

    environment = {
        "PYTHONPATH"                            : os.environ["PYTHONPATH"],
        "SPIDEROAK_MULTI_NODE_NAME"             : node_name,
        "DIYAPI_EVENT_PUBLISHER_PULL_ADDRESS"   : pull_address,
        "DIYAPI_EVENT_PUBLISHER_PUB_ADDRESS"    : pub_address,
    }        

    log.info("starting %s %s" % (args, environment, ))
    return subprocess.Popen(args, stderr=subprocess.PIPE, env=environment)

