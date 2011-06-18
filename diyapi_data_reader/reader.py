# -*- coding: utf-8 -*-
"""
reader.py

read segment data for avatars
"""
import logging

from diyapi_tools.data_definitions import segment_row_template, \
        segment_sequence_template

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
    print result
    return [segment_row_template._make(row) for row in result]

class Reader(object):
    """
    read segment data for avatars
    """
    def __init__(self, connection, repository_path):
        self._log = logging.getLogger("Reader")
        self._connection = connection
        self._repository_path = repository_path

    def get_file_information(self, avatar_id, key):
        """
        retrieve file specific information about the segment
        there can be more than one row per file, both dues to versions
        (timestamp) and handoffs (segment_num)
        """
        return _retrieve_segment_rows(self._connection, avatar_id, key)

