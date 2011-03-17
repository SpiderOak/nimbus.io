# -*- coding: utf-8 -*-
"""
util.py

utility functions for unit tests
"""
import random
import time
import os
import os.path

from diyapi_database_server import database_content

def random_string(size):
    with open('/dev/urandom', 'rb') as f:
        return f.read(size)

def generate_key():
    """generate a unique key for data storage"""
    n = 0
    while True:
        n += 1
        yield "test-key-%06d" % (n, )

def generate_database_content(timestamp=time.time(), version_number=0, segment_number=1):
    return database_content.factory(
        timestamp=timestamp, 
        is_tombstone=False,  
        version_number=version_number,
        segment_number=segment_number,  
        segment_size=42,  
        segment_count=1,
        total_size=4200,  
        file_adler32=345, 
        file_md5="ffffffffffffffff",
        segment_adler32=123, 
        segment_md5="1111111111111111",
        file_name="aaa"
    )

def identify_program_dir(target_dir):
    python_path = os.environ["PYTHONPATH"]
    for work_path in python_path.split(os.pathsep):
        test_path = os.path.join(work_path, target_dir)
        if os.path.isdir(test_path):
            return test_path

    raise ValueError(
        "Can't find %s in PYTHONPATH '%s'" % (target_dir, python_path, )
    )

