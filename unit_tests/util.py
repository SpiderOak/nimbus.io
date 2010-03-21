# -*- coding: utf-8 -*-
"""
util.py

utility functions for unit tests
"""
import random

def random_string(size):
    return "".join([chr(random.randrange(255)) for _ in xrange(size)])

def generate_key():
    """generate a unique key for data storage"""
    n = 0
    while True:
        n += 1
        yield "test-key-%06d" % (n, )

