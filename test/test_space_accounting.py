# -*- coding: utf-8 -*-
"""
test_space_accounting.py


"""
import time

import amqplib.client_0_8 as amqp

from diyapi_tools import amqp_connection
from messages.space_accounting_detail import SpaceAccountingDetail

if __name__ == "__main__":
    print "start test"
    message = SpaceAccountingDetail(
        1001,
        time.time(),
        SpaceAccountingDetail.bytes_added,
        42
    )
    amqp_message = amqp.Message(message.marshall())
    connection = amqp_connection.open_connection()
    channel = connection.channel()
    channel.basic_publish( 
        amqp_message, 
        exchange=amqp_connection.space_accounting_exchange_name, 
        routing_key=SpaceAccountingDetail.routing_key,
        mandatory = True
    )
    channel.close()
    connection.close()
    print "message sent"

