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
            timestamp=time.time(), 
            is_tombstone=False,  
            segment_number=1,  
            segment_size=42,  
            total_size=4200,  
            adler32=345, 
            md5="ffffffffffffffff" 
        )
        marshalled = database_content.marshall(original)
        unmarshalled = database_content.unmarshall(marshalled)
        self.assertEquals(unmarshalled, original)

if __name__ == "__main__":
    unittest.main()

