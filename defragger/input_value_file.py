# -*- coding: utf-8 -*-
"""
A value file for existing sequences, to be removed or renamed after defrag
"""
import logging

from tools.data_definitions import compute_value_file_path

class InputValueFile(object):
    """
    A value file for existing sequences, to be removed or renamed after defrag
    """
    def __init__(self, repository_path, value_file_row): 
        self._log = logging.getLogger("InputValueFile")
        self._repository_path = repository_path
        self._value_file_row = value_file_row
        self._value_file_path = \
                compute_value_file_path(repository_path, 
                                        value_file_row.space_id, 
                                        value_file_row.id)
        self._value_file = open(self._value_file_path, "rb")

    def close(self):
        """
        close the value file
        """
        self._value_file.close()

    @property
    def value_file_id(self):
        """
        value_file_id
        """
        return self._value_file_row.id

    @property
    def value_file_path(self):
        """
        value_file_path
        """
        return self._value_file_path

    def read(self, offset, size):
        """
        return data from the file
        """
        self._value_file.seek(offset)
        return self._value_file.read(size)

