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
            file_adler32=345, 
            file_md5='\x936\xeb\xf2P\x87\xd9\x1c\x81\x8e\xe6\xe9\xec)\xf8\xc1',
            segment_adler32=123, 
            segment_md5="1111111111111111",
            file_name="amblsmp0555"
        )
        marshalled = database_content.marshall(original)
        pos = 0
        (unmarshalled, pos) = database_content.unmarshall(marshalled, pos)
        self.assertEquals(unmarshalled, original)
        self.assertEquals(pos, len(marshalled))

if __name__ == "__main__":
    unittest.main()

