# -*- coding: utf-8 -*-
"""
test_handoff_server.py

test the handoff server process
"""
from base64 import b64encode
import hashlib
import logging
import os
import os.path
import shutil
import random
import sys
import time
import unittest
import zlib

import gevent
from gevent_zeromq import zmq

from diyapi_tools.standard_logging import initialize_logging
from diyapi_tools.greenlet_zeromq_pollster import GreenletZeroMQPollster
from diyapi_tools.greenlet_resilient_client import GreenletResilientClient
from diyapi_tools.greenlet_pull_server import GreenletPULLServer
from diyapi_tools.deliverator import Deliverator
from diyapi_tools.data_definitions import create_timestamp

from diyapi_web_server.data_writer_handoff_client import \
        DataWriterHandoffClient

from unit_tests.util import random_string, \
        generate_key, \
        start_data_writer, \
        start_data_reader, \
        start_handoff_server, \
        poll_process, \
        terminate_process

_log_path = "/var/log/pandora/test_handoff_server.log"
_test_dir = os.path.join("/tmp", "handoff_server_test_dir")
_node_count = 10
_data_writer_base_port = 8100
_data_reader_base_port = 8300
_handoff_server_base_port = 8700

def _generate_node_name(node_index):
    return "multi-node-%02d" % (node_index+1, )

_local_node_index = 0
_local_node_name = _generate_node_name(_local_node_index)
_node_names = [_generate_node_name(i) for i in range(_node_count)]

_data_writer_addresses = [
    "tcp://127.0.0.1:%s" % (_data_writer_base_port+i, ) \
    for i in range(_node_count)
]
_data_writer_pipeline_addresses = [
    "ipc:///tmp/spideroak-diyapi-data-writer-pipeline-%s/socket" % (
        _generate_node_name( i ),
    ) \
    for i in range(_node_count)
]
_data_reader_addresses = [
    "tcp://127.0.0.1:%s" % (_data_reader_base_port+i, ) \
    for i in range(_node_count)
]
_data_reader_pipeline_addresses = [
    "ipc:///tmp/spideroak-diyapi-data-reader-pipeline-%s/socket" % (
        _generate_node_name( i ),
    ) \
    for i in range(_node_count)
]
_handoff_server_addresses = [
    "ipc:///tmp/spideroak-diyapi-handoff_server-%s/socket" % (
        _generate_node_name( i ),
    ) \
    for i in range(_node_count)
]
_handoff_server_pipeline_addresses = [
    "tcp://127.0.0.1:%s" % (_handoff_server_base_port+i, ) \
    for i in range(_node_count)
]
_client_address = "tcp://127.0.0.1:8900"

def _repository_path(node_name):
    return os.path.join(_test_dir, node_name)

class TestHandoffServer(unittest.TestCase):
    """test message handling in handoff server"""

    def setUp(self):
        if not hasattr(self, "_log"):
            self._log = logging.getLogger("TestHandoffServer")

        self.tearDown()
        self._key_generator = generate_key()

        self._data_writer_processes = list()
        self._data_reader_processes = list()
        self._handoff_server_processes = list()

        for i in xrange(_node_count):
            node_name = _generate_node_name(i)
            repository_path = _repository_path(node_name)
            os.makedirs(repository_path)
            
            process = start_data_writer(
                node_name, 
                _data_writer_addresses[i],
                _data_writer_pipeline_addresses[i],
                repository_path
            )
            poll_result = poll_process(process)
            self.assertEqual(poll_result, None)
            self._data_writer_processes.append(process)
            time.sleep(1.0)

            process = start_data_reader(
                node_name, 
                _data_reader_addresses[i],
                _data_reader_pipeline_addresses[i],
                repository_path
            )
            poll_result = poll_process(process)
            self.assertEqual(poll_result, None)
            self._data_reader_processes.append(process)
            time.sleep(1.0)

            process = start_handoff_server(
                _node_names,
                node_name, 
                _handoff_server_addresses,
                _handoff_server_pipeline_addresses[i],
                _data_reader_addresses,
                _data_writer_addresses,
                _repository_path(node_name)
            )
            poll_result = poll_process(process)
            self.assertEqual(poll_result, None)
            self._handoff_server_processes.append(process)
            time.sleep(1.0)

        self._context = zmq.context.Context()
        self._pollster = GreenletZeroMQPollster()
        self._deliverator = Deliverator()

        self._pull_server = GreenletPULLServer(
            self._context, 
            _client_address,
            self._deliverator
        )
        self._pull_server.register(self._pollster)

        backup_nodes = random.sample(_node_names, 2)
        self._log.debug("backup nodes = %s" % (backup_nodes, ))

        self._resilient_clients = list()        
        for node_name, address in zip(_node_names, _data_writer_addresses):
            if not node_name in backup_nodes:
                continue
            resilient_client = GreenletResilientClient(
                self._context,
                self._pollster,
                node_name,
                address,
                _local_node_name,
                _client_address,
                self._deliverator,
            )
            self._resilient_clients.append(resilient_client)
        self._log.debug("%s resilient clients" % (
            len(self._resilient_clients), 
        ))

        self._data_writer_handoff_client = DataWriterHandoffClient(
            _node_names[0],
            self._resilient_clients
        )

        self._pollster.start()

    def tearDown(self):
        if hasattr(self, "_handoff_server_processes") \
        and self._handoff_server_processes is not None:
            print >> sys.stderr, "terminating _handoff_server_processes"
            for process in self._handoff_server_processes:
                terminate_process(process)
            self._handoff_server_processes = None
        if hasattr(self, "_data_writer_processes") \
        and self._data_writer_processes is not None:
            print >> sys.stderr, "terminating _data_writer_processes"
            for process in self._data_writer_processes:
                terminate_process(process)
            self._data_writer_processes = None
        if hasattr(self, "_data_reader_processes") \
        and self._data_reader_processes is not None:
            print >> sys.stderr, "terminating _data_reader_processes"
            for process in self._data_reader_processes:
                terminate_process(process)
            self._data_reader_processes = None

        if hasattr(self, "_pollster") \
        and self._pollster is not None:
            print >> sys.stderr, "terminating _pollster"
            self._pollster.kill()
            self._pollster.join(timeout=3.0)
            self._pollster = None
        
        if hasattr(self, "_pull_server") \
        and self._pull_server is not None:
            print >> sys.stderr, "terminating _pull_server"
            self._pull_server.close()
            self._pull_server = None
 
        if hasattr(self, "_resilient_clients") \
        and self._resilient_clients is not None:
            print >> sys.stderr, "terminating _resilient_clients"
            for client in self._resilient_clients:
                client.close()
            self._resilient_clients = None
 
        if hasattr(self, "_context") \
        and self._context is not None:
            print >> sys.stderr, "terminating _context"
            self._context.term()
            self._context = None

        if os.path.exists(_test_dir):
            shutil.rmtree(_test_dir)

    def test_handoff_small_content(self):
        """test retrieving content that fits in a single message"""
        file_size = 10 * 64 * 1024
        file_content = random_string(file_size) 
        avatar_id = 1001
        key  = self._key_generator.next()
        timestamp = create_timestamp()
        segment_num = 5

        file_adler32 = zlib.adler32(file_content)
        file_md5 = hashlib.md5(file_content)

        message = {
            "message-type"      : "archive-key-entire",
            "avatar-id"         : avatar_id,
            "key"               : key, 
            "timestamp-repr"    : repr(timestamp),
            "segment-num"       : segment_num,
            "file-size"         : file_size,
            "file-adler32"      : file_adler32,
            "file-hash"         : b64encode(file_md5.digest()),
            "file-user-id"      : None,
            "file-group-id"     : None,
            "file-permissions"  : None,
            "handoff-node-id"   : None,
        }
        g = gevent.spawn(self._send_message_get_reply, message, file_content)
        g.join(timeout=10.0)
        self.assertEqual(g.ready(), True)
        reply = g.value
        self.assertEqual(reply["message-type"], "archive-key-final-reply")
        self.assertEqual(reply["result"], "success")

        print >> sys.stderr, "archive successful"
        print >> sys.stderr, "press [Enter] to continue" 
        raw_input()

#    def test_handoff_large_content(self):
#        """test handing off content that fits in a multiple messages"""
#        segment_size = 120 * 1024
#        chunk_count = 10
#        total_size = int(1.2 * segment_size * chunk_count)
#        avatar_id = 1001
#        test_data = [random_string(segment_size) for _ in range(chunk_count)]
#        key  = self._key_generator.next()
#        version_number = 0
#        segment_num = 5
#        sequence = 0
#        timestamp = time.time()
#
#        file_adler32 = -42
#        file_md5 = "ffffffffffffffff"
#        segment_adler32 = 32
#        segment_md5 = "1111111111111111"
#
#        message = {
#            "message-type"      : "archive-key-start",
#            "avatar-id"         : avatar_id,
#            "timestamp"         : timestamp,
#            "sequence"          : sequence,
#            "key"               : key, 
#            "version-number"    : version_number,
#            "segment-num"    : segment_num,
#            "segment-size"      : segment_size,
#        }
#        g = gevent.spawn(
#            self._send_message_get_reply, message, test_data[sequence]
#        )
#        g.join(timeout=10.0)
#        self.assertEqual(g.ready(), True)
#        reply = g.value
#        self.assertEqual(reply["message-type"], "archive-key-start-reply")
#        self.assertEqual(reply["result"], "success")
#
#        for content_item in test_data[1:-1]:
#            sequence += 1
#            message = {
#                "message-type"      : "archive-key-next",
#                "avatar-id"         : avatar_id,
#                "key"               : key,
#                "version-number"    : version_number,
#                "segment-num"    : segment_num,
#                "sequence"          : sequence,
#            }
#            g = gevent.spawn(
#                self._send_message_get_reply, message, test_data[sequence]
#            )
#            g.join(timeout=10.0)
#            self.assertEqual(g.ready(), True)
#            reply = g.value
#            self.assertEqual(reply["message-type"], "archive-key-next-reply")
#            self.assertEqual(reply["result"], "success")
#        
#        sequence += 1
#        message = {
#            "message-type"      : "archive-key-final",
#            "avatar-id"         : avatar_id,
#            "key"               : key,
#            "version-number"    : version_number,
#            "segment-num"    : segment_num,
#            "sequence"          : sequence,
#            "total-size"        : total_size,
#            "file-adler32"      : file_adler32,
#            "file-md5"          : b64encode(file_md5),
#            "segment-adler32"   : segment_adler32,
#            "segment-md5"       : b64encode(segment_md5),
#        }
#        g = gevent.spawn(
#            self._send_message_get_reply, message, test_data[sequence]
#        )
#        g.join(timeout=10.0)
#        self.assertEqual(g.ready(), True)
#        reply = g.value
#        self.assertEqual(reply["message-type"], "archive-key-final-reply")
#        self.assertEqual(reply["result"], "success")
#        self.assertEqual(reply["previous-size"], 0)
#
#        print >> sys.stderr, "archive successful: starting missing data writer"
#        self._start_missing_data_writer()
#        print >> sys.stderr, "data_writer started"
#        print >> sys.stderr, "press [Enter] to continue" 
#        raw_input()

    def _send_message_get_reply(self, message, content_item):
        completion_channel = \
            self._data_writer_handoff_client.queue_message_for_send(
                message, data=content_item
            )
        self._log.debug("before completion_channel.get()")
        reply, _ = completion_channel.get()
        self._log.debug("after completion_channel.get()")
        return reply

if __name__ == "__main__":
    initialize_logging(_log_path)
    unittest.main()

