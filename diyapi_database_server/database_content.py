# -*- coding: utf-8 -*-
"""
database_content.py

DatabaseContent, with marshalling and unmarshalling code
"""
from collections import namedtuple
import struct

_content_template = "!d?BIQL16s"

factory =  namedtuple(
    "DatabaseContent", [
        "timestamp", 
        "is_tombstone", 
        "segment_number", 
        "segment_size", 
        "total_size", 
        "adler32",
        "md5",
    ]
)

def marshall(content):
    """return a string of marshalled content"""
    return struct.pack(
        _content_template,
        content.timestamp, 
        content.is_tombstone, 
        content.segment_number, 
        content.segment_size, 
        content.total_size, 
        content.adler32,
        content.md5
    )

def unmarshall(data):
    """return a DatabaseContent tuple unmarshaled from raw data"""
    return factory._make(struct.unpack(_content_template, data))

