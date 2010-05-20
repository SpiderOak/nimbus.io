# -*- coding: utf-8 -*-
"""
space_accounting_database.py

wrap access to the diyapi_space_accounting_table 
"""
import logging

from diyapi_tools.pandora_database_connection import \
        get_pandora_database_connection

_insert_command = """
INSERT INTO diyapi_space_accounting
(avatar_id, timestamp, bytes_added, bytes_removed, bytes_retrieved)
VALUES(%s, '%s'::timestamp, %s, %s, %s);
""".strip()

class SpaceAccountingDatabase(object):
    """wrap access to the diyapi_space_accounting_table"""
    def __init__(self):
        self._log = logging.getLogger("SpaceAccountingDatabase")
        self._connection = get_pandora_database_connection()
        self._connection.execute("BEGIN;")

    def commit(self):
        """commit the updates and close the database connection"""
        self._connection.execute("COMMIT;")
        self._connection.close()

    def store_avatar_stats(
        self,
        avatar_id,
        timestamp,
        bytes_added,
        bytes_retrieved,
        bytes_removed
    ):
        """store one row for an avatar"""
        command = _insert_command % (
            avatar_id,
            timestamp,
            bytes_added,
            bytes_retrieved,
            bytes_removed,
        )
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

