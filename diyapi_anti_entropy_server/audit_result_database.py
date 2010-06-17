# -*- coding: utf-8 -*-
"""
audit_result_database.py

wrap access to the diyapi_audit_result_table 
"""
import logging

from diyapi_tools.pandora_database_connection import \
        get_pandora_database_connection

_insert_command = """
INSERT INTO diyapi_audit_result
(avatar_id, timestamp, bytes_added, bytes_removed, bytes_retrieved)
VALUES(%s, '%s'::timestamp, %s, %s, %s);
""".strip()

class AuditResultDatabase(object):
    """wrap access to the diyapi_audit_result table"""
    def __init__(self):
        self._log = logging.getLogger("AuditResultDatabase")
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
    audit_result_database = SpaceAccountingDatabase()
    audit_result_database.store_avatar_stats(
        1001, 
        datetime.datetime.now(),
        1,
        0, 
        1
    )
    audit_result_database.commit()
    print "test complete"

