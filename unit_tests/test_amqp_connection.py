# -*- coding: utf-8 -*-
"""
test_amqp_connecton.py

test tools/amqp_connection.py
"""
import logging
import unittest

from tools import amqp_connection

class TestAQMPConnection(unittest.TestCase):
    """test tools/amqp_connection.py"""

    def test_connect(self):
        connection = amqp_connection.open_connection()
        self.assertNotEqual(connection, None)
        channel = connection.channel()
        amqp_connection.create_exchange(channel)
        amqp_connection.verify_exchange(channel)
        channel.close()
        connection.close()

if __name__ == "__main__":
    unittest.main()
