# -*- coding: utf-8 -*-
"""
data_slicer.py

A class that splits an uploaded file into slices.
"""


class DataSlicer(object):
    """Iterator that reads data from a file-like object and yields slices."""

    def __init__(self, file_obj, slice_size):
        self.file_obj = file_obj
        self.slice_size = slice_size
        self.buf = ''

    def __iter__(self):
        return self

    def next(self):
        while len(self.buf) < self.slice_size:
            data = self.file_obj.read(self.slice_size)
            if not data:
                if self.buf:
                    data = self.buf
                    self.buf = ''
                    return data
                raise StopIteration()
            self.buf += data
        data = self.buf[:self.slice_size]
        self.buf = self.buf[self.slice_size:]
        return data
