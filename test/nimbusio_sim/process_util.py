# -*- coding: utf-8 -*-
"""
process_util.py

utility functions for managing processes
"""
import logging
import os
import os.path
import subprocess
import sys

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
        if process.stderr is not None:
            print >> sys.stderr, process.stderr.read()
        print >> sys.stderr, "-" * 15
        print >> sys.stderr, process.returncode

def start_event_aggregator(environment):
    log = logging.getLogger("start_event_aggregator")
    server_dir = identify_program_dir(u"event_aggregator")
    server_path = os.path.join(server_dir, "event_aggregator_main.py")
    
    args = [
        sys.executable,
        server_path,
    ]

    log.info("starting %s %s" % (args, environment, ))
    return subprocess.Popen(args, stderr=subprocess.PIPE, env=environment)

def start_event_subscriber(environment):
    log = logging.getLogger("start_event_subscriber")
    server_dir = identify_program_dir(u"test")
    server_path = os.path.join(server_dir, "event_subscriber.py")
    
    args = [
        sys.executable,
        server_path,
        "error"
    ]

    log.info("starting %s %s" % (args, environment, ))
    return subprocess.Popen(args, stderr=subprocess.PIPE, env=environment)

def start_data_writer(node_name, environment, profile):
    log = logging.getLogger("start_data_writer %s profile=%r" % ( 
        node_name, profile
    ))
    server_dir = identify_program_dir(u"data_writer")
    server_path = os.path.join(server_dir, "data_writer_main.py")
    
    args = [
        sys.executable,
        server_path,
    ]

    if profile:
        profile_path = os.path.join(
            environment["NIMBUSIO_PROFILE_DIR"],
            "data_writer_%s.pstats" % (node_name, )
        )
        args[1:1] = [
            "-m",
            "cProfile",
            "-o",
            profile_path
        ]

    log.info("starting %s %s" % (args, environment, ))
    return subprocess.Popen(args, stderr=subprocess.PIPE, env=environment)

def start_data_reader(node_name, environment, profile):
    log = logging.getLogger("start_data_reader %s profile = %r" % (
        node_name, profile, 
    ))
    server_dir = identify_program_dir(u"data_reader")
    server_path = os.path.join(server_dir, "data_reader_main.py")
    
    args = [
        sys.executable,
        server_path,
    ]

    if profile:
        profile_path = os.path.join(
            environment["NIMBUSIO_PROFILE_DIR"],
            "data_reader_%s.pstats" % (node_name, )
        )
        args[1:1] = [
            "-m",
            "cProfile",
            "-o",
            profile_path
        ]

    log.info("starting %s %s" % (args, environment, ))
    return subprocess.Popen(args, stderr=subprocess.PIPE, env=environment)

def start_anti_entropy_server(node_name, environment, profile):
    log = logging.getLogger("_start_anti_entropy_server%s" % (node_name, ))
    server_dir = identify_program_dir(u"anti_entropy_server")
    server_path = os.path.join(
        server_dir, "anti_entropy_server_main.py"
    )
    
    args = [
        sys.executable,
        server_path,
    ]

    if profile:
        profile_path = os.path.join(
            environment["NIMBUSIO_PROFILE_DIR"],
            "anti_entropy_server_%s.pstats" % (node_name, )
        )
        args[1:1] = [
            "-m",
            "cProfile",
            "-o",
            profile_path
        ]

    log.info("starting %s %s" % (args, environment, ))
    return subprocess.Popen(args, stderr=subprocess.PIPE, env=environment)

def start_space_accounting_server(node_name, environment):
    log = logging.getLogger("_start_space_accounting_server%s" % (node_name, ))
    server_dir = identify_program_dir(u"space_accounting_server")
    server_path = os.path.join(
        server_dir, "space_accounting_server_main.py"
    )
    
    args = [
        sys.executable,
        server_path,
    ]

    log.info("starting %s %s" % (args, environment, ))
    return subprocess.Popen(args, stderr=subprocess.PIPE, env=environment)

def start_handoff_server(node_name, environment, profile):
    log = logging.getLogger("start_handoff_server_%s" % (node_name, ))
    server_dir = identify_program_dir(u"handoff_server")
    server_path = os.path.join(server_dir, "handoff_server_main.py")
    
    args = [
        sys.executable,
        server_path,
    ]

    if profile:
        profile_path = os.path.join(
            environment["NIMBUSIO_PROFILE_DIR"],
            "handoff_server_%s.pstats" % (node_name, )
        )
        args[1:1] = [
            "-m",
            "cProfile",
            "-o",
            profile_path
        ]

    log.info("starting %s %s" % (args, environment, ))
    return subprocess.Popen(args, stderr=subprocess.PIPE, env=environment)

def start_event_publisher(node_name, environment):
    log = logging.getLogger("start_event_publisher_%s" % (node_name, ))
    server_dir = identify_program_dir(u"event_publisher")
    server_path = os.path.join(server_dir, "event_publisher_main.py")
    
    args = [
        sys.executable,
        server_path,
    ]

    log.info("starting %s %s" % (args, environment, ))
    return subprocess.Popen(args, stderr=subprocess.PIPE, env=environment)

def start_performance_packager(node_name, environment):
    log = logging.getLogger("start_performance_packager_%s" % (node_name, ))
    server_dir = identify_program_dir(u"performance_packager")
    server_path = os.path.join(
        server_dir, "performance_packager_main.py"
    )
    
    args = [
        sys.executable,
        server_path,
    ]

    log.info("starting %s %s" % (args, environment, ))
    return subprocess.Popen(args, stderr=subprocess.PIPE, env=environment)

def start_stats_subscriber(environment):
    log = logging.getLogger("start_stats_subscriber")
    server_dir = identify_program_dir(u"test")
    server_path = os.path.join(
        server_dir, "stats_subscriber.py"
    )
    
    args = [
        sys.executable,
        server_path,
    ]

    log.info("starting %s %s" % (args, environment, ))
    return subprocess.Popen(args, stderr=subprocess.PIPE, env=environment)

def start_web_server(node_name, environment, profile):
    log = logging.getLogger("start_performance_packager_%s" % (node_name, ))
    server_dir = identify_program_dir(u"web_server")
    server_path = os.path.join(
        server_dir, "web_server_main.py"
    )
    args = [
        sys.executable,
        server_path,
    ]

    if profile:
        profile_path = os.path.join(
            environment["NIMBUSIO_PROFILE_DIR"],
            "web_server_%s.pstats" % (node_name, )
        )
        args[1:1] = [
            "-m",
            "cProfile",
            "-o",
            profile_path
        ]

    log.info("starting %s %s" % (args, environment, ))

    #the webserver writes enough stderr/out to fill up the pipe and make the
    #subprocess block.
    return subprocess.Popen(args, stderr=None, env=environment)

