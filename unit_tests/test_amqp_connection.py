# -*- coding: utf-8 -*-
"""
test_amqp_connecton.py

test tools/amqp_connection.py
"""
import logging
import unittest

from diyapi_tools import amqp_connection

class TestAQMPConnection(unittest.TestCase):
    """test tools/amqp_connection.py"""

    def test_connect(self):
        """test connection, channel, create_exchange, verify_exchange"""
        connection = amqp_connection.open_connection()
        self.assertNotEqual(connection, None)
        channel = connection.channel()
        amqp_connection.create_exchange(channel)
        amqp_connection.verify_exchange(channel)
        channel.close()
        connection.close()

if __name__ == "__main__":
    unittest.main()
