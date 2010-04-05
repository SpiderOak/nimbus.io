# -*- coding: utf-8 -*-
"""
test_database_content.py

test diyapi_database_server/database_content.py
"""
import logging
import time
import unittest

from diyapi_database_server import database_content

class TestDatabaseContent(unittest.TestCase):
    """test diyapi_database_server/database_content.py"""

    def test_marshall_and_unmarshall(self):
        """test that we can unmarshall content we have marshalled"""
        original = database_content.factory(
            is_tombstone=False,  
            timestamp=time.time(), 
            version_number=42,
            segment_number=4,  
            segment_count=1,
            segment_size=42,  
            total_size=4200,  
            adler32=345, 
            md5="ffffffffffffffff",
            file_name="amblsmp0555"
        )
        marshalled = database_content.marshall(original)
        pos = 0
        (unmarshalled, pos) = database_content.unmarshall(marshalled, pos)
        self.assertEquals(unmarshalled, original)
        self.assertEquals(pos, len(marshalled))

if __name__ == "__main__":
    unittest.main()

