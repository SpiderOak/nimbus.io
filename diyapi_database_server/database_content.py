# -*- coding: utf-8 -*-
"""
database_content.py

DatabaseContent, with marshalling and unmarshalling code
"""
from collections import namedtuple
import struct

from tools.marshalling import marshall_string, unmarshall_string

# d - timestamp
# ? - is_tombstone
# B - segment_number
# I - segment size
# I - segment count
# Q - total_size
# L - adler32
#16s- md5 
_content_template = "d?BIIQL16s"
_content_template_size = struct.calcsize(_content_template)

factory =  namedtuple(
    "DatabaseContent", [
        "timestamp", 
        "is_tombstone", 
        "segment_number", 
        "segment_count",
        "segment_size", 
        "total_size", 
        "adler32",
        "md5",
        "file_name",
    ]
)

def create_tombstone(timestamp):
    """create a 'tombstone' database entry wiht a specified timestamp"""
    return factory(
        timestamp = timestamp, 
        is_tombstone = True, 
        segment_number = 0, 
        segment_count = 0,
        segment_size = 0 ,
        total_size = 0, 
        adler32 = 0,
        md5 = "",
        file_name = "",
    )

def marshall(content):
    """
    return a string of marshalled content
    encode the segment number in the first byte
    """
    packed_content = struct.pack(
        _content_template,
        content.timestamp, 
        content.is_tombstone & 0xFF, 
        content.segment_number & 0xFF,
        content.segment_count & 0xFFFFFFFF,
        content.segment_size & 0xFFFFFFFF, 
        content.total_size, 
        content.adler32 & 0xFFFFFFFF,
        content.md5
    )
    packed_file_name = marshall_string(content.file_name)
    return "".join([
        chr(content.segment_number & 0xFF), packed_content, packed_file_name, 
    ])

def unmarshall(data, pos):
    """
    unmarshall DatabaseContent tuple
    return (DatabaseContent, pos)
    with pos pointing one character after the marshalled tuple
    """
    # the first byte is the segment_number
    pos += 1
    content = struct.unpack(
        _content_template, data[pos:pos+_content_template_size]
    )
    pos += _content_template_size
    (file_name, pos, ) = unmarshall_string(data, pos)
    total_content = list(content)
    total_content.append(file_name)
    return (factory._make(total_content), pos)

def segment_number(data):
    """
    return the segment number (1..10) of the item without fully unpacking it
    """ 
    return ord(data[0])

