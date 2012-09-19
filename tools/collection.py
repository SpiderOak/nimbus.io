# -*- coding: utf-8 -*-
"""
collection.py

tools for managing the colection table
"""
from collections import namedtuple
import re

class CollectionError(Exception):
    pass
class UnparseableCollection(CollectionError):
    pass

_default_collection_prefix = "dd"

_host_collection_name_re = re.compile(
    r'(?P<collection_name>[^.]+)\.nimbus.io(?::\d+)?$'
)
_collection_name_re = re.compile(r'[a-z0-9][a-z0-9-]*[a-z0-9]$')
_max_collection_name_size = 63
_collection_entry_template = namedtuple(
    "CollectionEntry",
    ["collection_name", "collection_id", "username", "versioning", ]
)

def valid_collection_name(collection_name):
    """
    return True if the username is valid
    """
    return len(collection_name) <= _max_collection_name_size \
        and not '--' in collection_name \
        and _collection_name_re.match(collection_name) is not None

def compute_default_collection_name(username):
    """
    return the name of the customer's default collection, based on username
    """
    return "-".join([_default_collection_prefix, username, ])

