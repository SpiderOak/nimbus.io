# -*- coding: utf-8 -*-
"""
handoff_repository.py

A module for store and retrieving handoff hints
"""
from collections import namedtuple
import logging


factory =  namedtuple(
    "Handoff", [
        "timestamp", 
        "key",
        "version_number",
        "segment_number", 
    ]
)


def store(dest_exchange, timestamp, key, version_number, segment_number):
    """
    store a hint to handoff this archive to dest exchange when its
    data_writer announces its presence.
    """

def handoffs_exist(dest_exchange):
    """return True if we have handoffs for this exchange"""
    return False

def iterate_handoffs(dest_exchange):
    """
    create a generator that returns handoffs for this exchange in the order
    they were received.
    """
