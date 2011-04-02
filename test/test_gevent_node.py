# -*- coding: utf-8 -*-
"""
a test of using gevent with zmq
"""
import os
import sys
import uuid

import gevent
from gevent_zeromq import zmq

from diyapi_tools.standard_logging import initialize_logging
from diyapi_tools.greenlet_zeromq_pollster import GreenletZeroMQPollster
from diyapi_tools.greenlet_xreq_client import GreenletXREQClient

_database_server_address = os.environ["DIYAPI_DATABASE_SERVER_ADDRESS"]

def _database_request(database_client, message):
    print "_database_request"
    delivery_channel = database_client.queue_message_for_send(message)
    print "delivery_channel.get()"
    reply = delivery_channel.get()
    return reply

def main():
    """main entry point"""
    avatar_id = 1001
    key = "splort"

    initialize_logging("/var/log/pandora/test_node.log")

    print "starting test"
    context = zmq.context.Context()

    pollster = GreenletZeroMQPollster()
    database_client = GreenletXREQClient(
        context, "node01", _database_server_address
    )
    database_client.register(pollster)
    print "starting pollster"
    pollster.start()

    request_id = uuid.uuid1().hex
    message = {
        "message-type"      : "key-list",
        "request-id"        : request_id,
        "avatar-id"         : avatar_id,
        "key"               : key, 
    }

    print "spawning database request"
    greenlet = gevent.spawn(_database_request, database_client, message)

    print "joining greenlet"
    greenlet.join()
    print "reply =", greenlet.value

    print "shutting down"
    database_client.close()
    context.term()

if __name__  == "__main__":
    sys.exit(main())

