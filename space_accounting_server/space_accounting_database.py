# -*- coding: utf-8 -*-
"""
space_accounting_database.py

wrap access to the space_accounting_table 
"""
import logging

from tools.database_connection import get_central_connection

class SpaceAccountingDatabaseError(Exception):
    pass
class SpaceAccountingDatabaseCollectionNotFound(SpaceAccountingDatabaseError):
    pass

_insert_command = """
INSERT INTO nimbusio_central.space_accounting
(collection_id, timestamp, bytes_added, bytes_removed, bytes_retrieved)
VALUES(%s, '%s'::timestamp, %s, %s, %s);
""".strip()

_collection_query = """
SELECT COALESCE(SUM(bytes_added), 0), 
COALESCE(SUM(bytes_removed), 0), 
COALESCE(SUM(bytes_retrieved), 0)
FROM nimbusio_central.space_accounting 
WHERE collection_id = %s
""".strip()

_clear_command = """
DELETE FROM nimbusio_central.space_accounting 
WHERE collection_id = %s
""".strip()

class SpaceAccountingDatabase(object):
    """wrap access to the space_accounting_table"""
    def __init__(self, transaction=True):
        self._log = logging.getLogger("SpaceAccountingDatabase")
        self._connection = get_central_connection()
        if transaction:
            self._connection.execute("BEGIN;")

    def commit(self):
        """commit the updates and close the database connection"""
        self._connection.execute("COMMIT;")
        self._connection.close()

    def close(self):
        self._connection.close()

    def store_collection_stats(
        self,
        collection_id,
        timestamp,
        bytes_added,
        bytes_removed,
        bytes_retrieved
    ):
        """store one row for an collection"""
        command = _insert_command % (
            collection_id,
            timestamp,
            bytes_added,
            bytes_removed,
            bytes_retrieved,
        )
        self._connection.execute(command)
    
    def retrieve_collection_stats(self, collection_id):
        """get the consolidated stats for an collection"""
        query = _collection_query % (collection_id, )
        result = self._connection.fetch_one_row(query)
        if result is None:
            raise SpaceAccountingDatabasecollectionNotFound(str(collection_id))
        [bytes_added, bytes_removed, bytes_retrieved, ] = result
        return bytes_added, bytes_removed, bytes_retrieved, 

    def clear_collection_stats(self, collection_id):
        """clear all stats for an collection *** for use in testing ***"""
        command = _clear_command % (collection_id, )
        self._connection.execute(command)

if __name__ == "__main__":
    import datetime
    print
    print "testing"
    space_accounting_database = SpaceAccountingDatabase()
    space_accounting_database.store_collection_stats(
        1001, 
        datetime.datetime.now(),
        1,
        0, 
        1
    )
    space_accounting_database.commit()
    print "test complete"

