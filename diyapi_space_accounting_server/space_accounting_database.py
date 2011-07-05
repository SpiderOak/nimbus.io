# -*- coding: utf-8 -*-
"""
space_accounting_database.py

wrap access to the diyapi_space_accounting_table 
"""
import logging

from diyapi_tools.pandora_database_connection import \
        get_database_connection

class SpaceAccountingDatabaseError(Exception):
    pass
class SpaceAccountingDatabaseAvatarNotFound(SpaceAccountingDatabaseError):
    pass

_insert_command = """
INSERT INTO diyapi_space_accounting
(avatar_id, timestamp, bytes_added, bytes_removed, bytes_retrieved)
VALUES(%s, '%s'::timestamp, %s, %s, %s);
""".strip()

_avatar_query = """
SELECT COALESCE(SUM(bytes_added), 0), 
COALESCE(SUM(bytes_removed), 0), 
COALESCE(SUM(bytes_retrieved), 0)
FROM diyapi_space_accounting 
WHERE avatar_id = %s
""".strip()

_clear_command = """
DELETE FROM diyapi_space_accounting 
WHERE avatar_id = %s
""".strip()

class SpaceAccountingDatabase(object):
    """wrap access to the diyapi_space_accounting_table"""
    def __init__(self, transaction=True):
        self._log = logging.getLogger("SpaceAccountingDatabase")
        self._connection = get_database_connection()
        if transaction:
            self._connection.execute("BEGIN;")

    def commit(self):
        """commit the updates and close the database connection"""
        self._connection.execute("COMMIT;")
        self._connection.close()

    def close(self):
        self._connection.close()

    def store_avatar_stats(
        self,
        avatar_id,
        timestamp,
        bytes_added,
        bytes_removed,
        bytes_retrieved
    ):
        """store one row for an avatar"""
        command = _insert_command % (
            avatar_id,
            timestamp,
            bytes_added,
            bytes_removed,
            bytes_retrieved,
        )
        self._connection.execute(command)
    
    def retrieve_avatar_stats(self, avatar_id):
        """get the consolidated stats for an avatar"""
        query = _avatar_query % (avatar_id, )
        result = self._connection.fetch_one_row(query)
        if result is None:
            raise SpaceAccountingDatabaseAvatarNotFound(str(avatar_id))
        [bytes_added, bytes_removed, bytes_retrieved, ] = result
        return bytes_added, bytes_removed, bytes_retrieved, 

    def clear_avatar_stats(self, avatar_id):
        """clear all stats for an avatar *** for use in testing ***"""
        command = _clear_command % (avatar_id, )
        self._connection.execute(command)

if __name__ == "__main__":
    import datetime
    print
    print "testing"
    space_accounting_database = SpaceAccountingDatabase()
    space_accounting_database.store_avatar_stats(
        1001, 
        datetime.datetime.now(),
        1,
        0, 
        1
    )
    space_accounting_database.commit()
    print "test complete"

