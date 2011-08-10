# -*- coding: utf-8 -*-
"""
amqp_connection.py

AMQP message sender connection to our local host
"""
import os

import amqplib.client_0_8 as amqp

_amqp_user = "spideroak"
_amqp_password = "spideroak"
_amqp_vhost = "spideroak_vhost"

_amqp_host = "localhost"
_amqp_port = int(os.environ.get("SPIDEROAK_AMQP_PORT", "5672"))
_socket_connect_timeout = float(
    os.environ.get("SPIDEROAK_AMQP_CONNECT_TIMEOUT", "1.0")
)
exchange_template = "%s-exchange"
_local_node_name = os.environ["SPIDEROAK_MULTI_NODE_NAME"]
local_exchange_name = exchange_template % (_local_node_name, )
broadcast_exchange_name = "spideroak_diyapi_broadcast"
space_accounting_exchange_name = "spideroak_diyapi_space_accounting"

def open_connection():
    """
    Return an AMQP connection to our local host.
    """
    return amqp.Connection( 
        host = "%s:%s" % (_amqp_host, _amqp_port, ), 
        userid = _amqp_user, 
        password = _amqp_password, 
        virtual_host = _amqp_vhost,
        connect_timeout=_socket_connect_timeout,
        insist = True # connect to this server only, no redirects 
    )

def verify_exchange(channel):
    """raise an exception if our local exchange doesn't exist"""
    channel.exchange_declare(
        exchange=local_exchange_name, 
        type="topic",
        passive=True,
        durable=True,
        auto_delete=False
    )
    channel.exchange_declare(
        exchange=broadcast_exchange_name, 
        type="fanout",
        passive=True,
        durable=True,
        auto_delete=False
    )

def create_exchange(channel):
    """
    Create our local exchange. This will work ok if the exchange already
    exists
    """
    channel.exchange_declare(
        exchange=local_exchange_name, 
        type="topic",
        passive=False,
        durable=True,
        auto_delete=False
    )
    channel.exchange_declare(
        exchange=broadcast_exchange_name, 
        type="fanout",
        passive=False,
        durable=True,
        auto_delete=False
    )
    channel.exchange_declare(
        exchange=space_accounting_exchange_name, 
        type="topic",
        passive=False,
        durable=True,
        auto_delete=False
    )

