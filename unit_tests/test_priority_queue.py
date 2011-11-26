# -*- coding: utf-8 -*-
"""
test_priority_qeue.py

test handling messages from PriorityQueue
"""
import os
import unittest

from tools.priority_queue import PriorityQueue
from tools.data_definitions import message_format

class TestPriorityQeue(unittest.TestCase):
    """test the priority queue"""

    def setUp(self):
        self.tearDown()

    def tearDown(self):
        pass

    def test_empty_queue(self):
        """test operations on an empty queue"""
        queue = PriorityQueue()
        self.assertEqual(len(queue), 0)
        self.assertRaises(IndexError, queue.popleft)

    def test_single_entry(self):
        """test adding and removing a single entry"""
        queue = PriorityQueue()
        message = message_format(
            ident=None, control={"priority" : 0}, body=None
        )
        self.assertEqual(len(queue), 0)
        queue.append((message.control, message.body, ))
        self.assertEqual(len(queue), 1)
        retrieved_message, _retrieved_body = queue.popleft()
        self.assertEqual(retrieved_message, message.control)
        self.assertEqual(len(queue), 0)
        self.assertRaises(IndexError, queue.popleft)

    def test_message_order(self):
        """test that we get messages in the order we expect"""
        queue = PriorityQueue()
        messages = [
            message_format(
                ident=None, 
                control={"priority" : 0, "expected-order" : 0}, 
                body=None
            ),
            message_format(
                ident=None, 
                control={"priority" : 5, "expected-order" : 2}, 
                body=None
            ),
            message_format(
                ident=None, 
                control={"priority" : 3, "expected-order" : 1}, 
                body=None
            ),
        ]
        self.assertEqual(len(queue), 0)
        for message in messages:
            queue.append((message.control, message.body, ))
        self.assertEqual(len(queue), len(messages))

        for order in range(len(messages)):
            retrieved_message, _retrieved_data = queue.popleft()
            self.assertEqual(
                order, retrieved_message["expected-order"]
            )

        self.assertEqual(len(queue), 0)
        self.assertRaises(IndexError, queue.popleft)
if __name__ == "__main__":
    unittest.main()

