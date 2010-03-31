# -*- coding: utf-8 -*-
"""
util.py

utility functions for unit tests
"""
import random
import time

from diyapi_database_server import database_content

def random_string(size):
    return "".join([chr(random.randrange(255)) for _ in xrange(size)])

def generate_key():
    """generate a unique key for data storage"""
    n = 0
    while True:
        n += 1
        yield "test-key-%06d" % (n, )

def generate_database_content(timestamp=time.time()):
    return database_content.factory(
        timestamp=timestamp, 
        is_tombstone=False,  
        segment_number=1,  
        segment_size=42,  
        segment_count=1,
        total_size=4200,  
        adler32=345, 
        md5="ffffffffffffffff",
        file_name="aaa"
    )

