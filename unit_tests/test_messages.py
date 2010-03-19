# -*- coding: utf-8 -*-
"""
test_messages.py

test AMQP Messages
"""
import logging
import time
import unittest
import uuid

from diyapi_database_server import database_content
from messages.database_key_insert import DatabaseKeyInsert

class TestMessages(unittest.TestCase):
    """test AMQP Messages"""

    def test_database_key_insert(self):
        """test DatabaseKeyInsert"""
        original_content = database_content.factory(
            timestamp=time.time(), 
            is_tombstone=False,  
            segment_number=1,  
            segment_size=42,  
            total_size=4200,  
            adler32=345, 
            md5="ffffffffffffffff" 
        )
        original_request_id = uuid.uuid1().hex
        original_reply_exchange = "reply-exchange"
        original_reply_routing_key = "reply.routing-key"
        original_key  = "abcdefghijk"
        message = DatabaseKeyInsert(
            original_request_id,
            original_reply_exchange,
            original_reply_routing_key,
            original_key, 
            original_content
        )
        marshalled_message = message.marshall()
        unmarshalled_message = DatabaseKeyInsert.unmarshall(marshalled_message)
        self.assertEqual(unmarshalled_message.request_id, original_request_id)
        self.assertEqual(
            unmarshalled_message.reply_routing_key, 
            original_reply_routing_key
        )
        self.assertEqual(
            unmarshalled_message.reply_exchange, original_reply_exchange
        )
        self.assertEqual(unmarshalled_message.key, original_key)
        self.assertEqual(unmarshalled_message.content, original_content)

if __name__ == "__main__":
    unittest.main()
