# -*- coding: utf-8 -*-
"""
common.py

common elements for the anti entropy server
"""
from collections import namedtuple
import time

retry_interval = 60.0 * 60.0

def retry_time():
    return time.time() + retry_interval

retry_entry_tuple = namedtuple("RetryEntry", [
    "retry_time", 
    "collection_id", 
    "row_id",
    "retry_count"
])

max_retry_count = 2
