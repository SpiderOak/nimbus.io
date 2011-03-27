# -*- coding: utf-8 -*-
"""
common.py

common elements for the anti entropy server
"""
retry_interval = 60.0 * 60.0

retry_entry_tuple = namedtuple("RetryEntry", [
    "retry_time", 
    "avatar_id", 
    "row_id",
    "retry_count"
])

max_retry_count = 2
