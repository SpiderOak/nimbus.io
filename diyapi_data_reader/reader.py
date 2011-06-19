# -*- coding: utf-8 -*-
"""
reader.py

read segment data for avatars
"""
import logging

from diyapi_tools.data_definitions import segment_row_template, \
        segment_sequence_template, \
        compute_value_file_path

def _retrieve_segment_rows(connection, avatar_id, key):
    """
    retrieve all rows for avatar-id and segment.
    Note that there is no unique constraint on (avatar_id, key):
    the caller must be prepared to deal with multiple rows
    """
    result = connection.fetch_all_rows("""
        select %s from diy.segment 
        where avatar_id = %%s and key = %%s
        order by timestamp desc, segment_num asc
    """ % (",".join(segment_row_template._fields), ), [avatar_id, key, ])
    return [segment_row_template._make(row) for row in result]

def _retrieve_sequence_rows(connection, avatar_id, segment_id):
    """
    retrieve all rows for avatar_id and segment
    """
    result = connection.fetch_all_rows("""
        select %s from diy.segment_sequence
        where avatar_id = %%s and segment_id = %%s
        order by sequence_num asc
    """ % (",".join(segment_sequence_template._fields), ), 
    [avatar_id, segment_id, ])
    return [segment_sequence_template._make(row) for row in result]

class Reader(object):
    """
    read segment data for avatars
    """
    def __init__(self, connection, repository_path):
        self._log = logging.getLogger("Reader")
        self._connection = connection
        self._repository_path = repository_path

    def get_segment_rows(self, avatar_id, key):
        """
        retrieve file specific information about the segment
        there can be more than one row per file, due both to versions
        (timestamp) and handoffs (segment_num)
        """
        return _retrieve_segment_rows(self._connection, avatar_id, key)
   
    def sequence_generator(self, avatar_id, segment_id):
        """
        a generator to return sequence data for a segment in order
        """
        open_value_files = dict()

        for sequence_row in _retrieve_sequence_rows(
            self._connection, avatar_id, segment_id
        ):
            if not sequence_row.value_file_id in open_value_files:
                open_value_files[sequence_row.value_file_id] = open(
                    compute_value_file_path(
                        self._repository_path, sequence_row.value_file_id
                    ), 
                    "r"
                )
            value_file = open_value_files[sequence_row.value_file_id]
            value_file.seek(sequence_row.value_file_offset)
            yield value_file.read(sequence_row.size)

        for value_file in open_value_files.values():            
            value_file.close()

