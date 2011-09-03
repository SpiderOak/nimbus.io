# -*- coding: utf-8 -*-
"""
reader.py

read segment data for collections
"""
import logging

from diyapi_tools.data_definitions import segment_row_template, \
        segment_sequence_template, \
        compute_value_file_path

def _all_segment_rows_for_key(connection, collection_id, key):
    """
    retrieve all rows for collection-id and key
    Note that there is no unique constraint on (collection_id, key):
    the caller must be prepared to deal with multiple rows
    """
    result = connection.fetch_all_rows("""
        select %s from nimbusio_node.segment 
        where collection_id = %%s and key = %%s
        order by timestamp desc, segment_num asc
    """ % (",".join(segment_row_template._fields), ), [collection_id, key, ])
    return [segment_row_template._make(row) for row in result]

def _all_sequence_rows_for_segment(
    connection, collection_id, key, timestamp, segment_num
):
    """
    retrieve all rows for a segment identified by 
    (collection_id, key, timestamp, segment_num)
    """
    result = connection.fetch_all_rows("""
        select %s from nimbusio_node.segment_sequence
        where segment_id = (
            select id from nimbusio_node.segment 
            where collection_id = %%s and key = %%s 
            and timestamp=%%s::timestamp and segment_num=%%s
        )
        order by sequence_num asc
    """ % (",".join(segment_sequence_template._fields), ), 
    [collection_id, key, timestamp, segment_num, ])
    return [segment_sequence_template._make(row) for row in result]

class Reader(object):
    """
    read segment data for collections
    """
    def __init__(self, connection, repository_path):
        self._log = logging.getLogger("Reader")
        self._connection = connection
        self._repository_path = repository_path

    def close(self):
        """have a close for consistency"""
        self._log.info("closing")

    def get_all_segment_rows_for_key(self, collection_id, key):
        """
        retrieve file specific information about the segment
        there can be more than one row per file, due both to versions
        (timestamp) and handoffs (segment_num)
        """
        return _all_segment_rows_for_key(self._connection, collection_id, key)
   
    def generate_all_sequence_rows_for_segment(
        self, collection_id, key, timestamp, segment_num
    ):
        """
        a generator to return sequence data for a segment in order
        """
        open_value_files = dict()

        sequence_rows = _all_sequence_rows_for_segment(
            self._connection, 
            collection_id, 
            key, 
            timestamp,
            segment_num
        )

        # first yield is count of sequences
        yield len(sequence_rows)

        for sequence_row in sequence_rows:
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

