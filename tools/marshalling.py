# -*- coding: utf-8 -*-
"""
marshalling.py

tools for marshalling
"""
import struct

_string_length_template = "I"
_string_length_size = struct.calcsize(_string_length_template)
_string_template = "%ds"

def marshall_string(string):
    """return a string containing the marshalled string"""
    size = len(string)
    packed_length = struct.pack(_string_length_template, size)
    packed_string = struct.pack(_string_template % (size, ), string)
    return "".join([packed_length, packed_string, ])

def unmarshall_string(data, pos):
    """
    rebuild string from data starting at pos
    return (string, pos, ) 
    with pos pointing one character after string in data
    """
    (size, ) = struct.unpack(
        _string_length_template, data[pos:pos+_string_length_size]
    )
    pos += _string_length_size
    (string, ) = struct.unpack(
        _string_template % (size, ), data[pos:pos+size]
    )
    pos += size

    return (string, pos, )

