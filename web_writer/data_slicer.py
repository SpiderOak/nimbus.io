# -*- coding: utf-8 -*-
"""
data_slicer.py

A generator that splits an uploaded body into slices.
"""
from tools.data_definitions import incoming_slice_size

def slice_generator(file_obj):
    buf = ''
    while True:
        data = file_obj.read(incoming_slice_size)
        if len(data) == 0:
            if len(buf) > 0:
                yield buf
            raise StopIteration()
        buf = "".join([buf, data, ])
        yield buf[:incoming_slice_size]
        buf = buf[incoming_slice_size:]

