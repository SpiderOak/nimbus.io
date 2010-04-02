# -*- coding: utf-8 -*-
"""
data_accumulator.py

A class that splits an uploaded file into slices.
"""


class DataAccumulator(object):
    """Reads data from a file-like object and splits it into slices."""

    def __init__(self, file_obj, slice_size, listener):
        self.file_obj = file_obj
        self.slice_size = slice_size
        self.listener = listener

    def accumulate(self):
        buf = ''
        while True:
            data = self.file_obj.read(self.slice_size)
            if not data:
                if buf:
                    self.listener.handle_slice(buf)
                return
            buf += data
            if len(buf) >= self.slice_size:
                self.listener.handle_slice(buf[:self.slice_size])
                buf = buf[self.slice_size:]
