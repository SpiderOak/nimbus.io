# -*- coding: utf-8 -*-
"""
reader.py

read key data for avatars
"""

def _retrieve_key_rows(connection, avatar_id, key):
    """
    retrieve all rows for avatar-id and key.
    Note that there is no unique constraint on (avatar_id, key):
    the caller must be prepared to deal with multiple rows
    """
    result = connection.fetch_all_rows("""
        select %s from diy.key 
        where avatar_id = %%s and name = %%s
        order by timestamp  
    """ % (",".join(key_row_template._fields), ), [avatar_id, key, ])
    print result
    return [key_row_template._make(row) for row in result]

class Reader(object):
    """
    read key data for avatars
    """
    def __init__(self):

    def start_retrieve(self, avatar_id, key, segment_number):
        """
        retrieve the known information about the segment
        """
        retrieved_rows = _retrieve_key_rows(
            self._database_connection, avatar_id, key
        )
        self.assertEqual(len(retrieved_rows), 1)
