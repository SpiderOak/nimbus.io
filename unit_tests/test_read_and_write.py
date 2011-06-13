# -*- coding: utf-8 -*-
"""
test_read_and_write.py

test writing and reading back 
"""
from collections import namedtuple
from datetime import datetime
import hashlib
import os
import os.path
import shutil
import time
import unittest

import psycopg2

from diyapi_tools.standard_logging import initialize_logging
from diyapi_tools.pandora_database_connection import get_node_local_connection

from diyapi_web_server.data_slicer import DataSlicer
from diyapi_web_server.zfec_segmenter import ZfecSegmenter
from diyapi_data_writer.output_value_file import OutputValueFile, \
        value_file_template

from unit_tests.util import random_string

_log_path = "/var/log/pandora/test_read_and_write.log"
_test_dir = os.path.join("/tmp", "test_read_and_write")
_repository_path = os.path.join(_test_dir, "diyapi")
_local_node_name = os.environ["SPIDEROAK_MULTI_NODE_NAME"]

def _retrieve_value_file_row(connection, value_file_id):
    result = connection.fetch_one_row("""
        select %s from diy.value_file 
        where id = %%s
    """ % (",".join(value_file_template._fields), ), [value_file_id, ])
    return value_file_template._make(result)

class TestReadAndWrite(unittest.TestCase):
    """test writing and reading back"""

    def setUp(self):
        self.tearDown()
        os.makedirs(_test_dir)

        self._database_connection = get_node_local_connection()

    def tearDown(self):
        if hasattr(self, "_database_connection") \
        and self._database_connection is not None:
            self._database_connection.close()
            self._database_connection = None

        if os.path.exists(_test_dir):
            shutil.rmtree(_test_dir)

    def test_simple_output_value_file(self):
        """test writing a simple output value file"""
        avatar_id = 1001
        key_id = 42
        data_size = 1024
        data = random_string(data_size)
        output_value_file = OutputValueFile(
            self._database_connection, _repository_path
        )
        self.assertEqual(output_value_file.size, 0)
        output_value_file.write_data_for_one_sequence(
            avatar_id, key_id, data
        )
        self.assertEqual(output_value_file.size, data_size)
        output_value_file.close()
        
        value_file_row = _retrieve_value_file_row(
            self._database_connection, output_value_file._value_file_id
        )

        self.assertEqual(value_file_row.size, data_size)
        data_md5_hash = hashlib.md5(data).digest()
        self.assertEqual(str(value_file_row.hash), data_md5_hash)
        self.assertEqual(value_file_row.sequence_count, 1)
        self.assertEqual(value_file_row.min_key_id, key_id)
        self.assertEqual(value_file_row.max_key_id, key_id)
        self.assertEqual(value_file_row.distinct_avatar_count, 1)
        self.assertEqual(value_file_row.avatar_ids, [avatar_id, ])

if __name__ == "__main__":
    initialize_logging(_log_path)
    unittest.main()

