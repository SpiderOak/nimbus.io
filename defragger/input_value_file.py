# -*- coding: utf-8 -*-
"""
A value file for existing sequences, to be removed or renamed after defrag
"""
import logging

from tools.data_definitions import compute_value_file_path, \
        value_file_template

class InputValueFile(object):
    def __init__(self, local_connection, repository_path, value_file_row): 
        self._log = logging.getLogger("InputValueFile")
        self._local_connection = local_connection
        self._repository_path = repository_path
        self._value_file_row = value_file_row
        value_file_path = compute_value_file_path(
            repository_path, value_file_row.id
        )
        self._value_file = open(value_file_path, "rb")

    def close(self):
        """
        close the value file
        """
        self._value_file.close()
        #TODO: move or delete the file

    @property
    def value_file_id(self):
        return self._value_file_row.id

    def read(self, offset, size):
        """
        return data from the file
        """
        self._value_file.seek(offset)
        return self._value_file.read(size)

