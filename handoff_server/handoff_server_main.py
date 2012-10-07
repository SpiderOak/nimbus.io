# -*- coding: utf-8 -*-
"""
handoff_server_main.py
"""
import logging
import os
import signal
import sys

from gevent import monkey
monkey.patch_all()

import gevent_zeromq
gevent_zeromq.monkey_patch()
from gevent_zeromq import zmq

import gevent_psycopg2
gevent_psycopg2.monkey_patch()

from gdbpool.interaction_pool import DBInteractionPool

import gevent
from gevent.pool import Group
from gevent.event import Event
from gevent.queue import Queue

from tools.standard_logging import initialize_logging
from tools.event_push_client import EventPushClient
from tools.database_connection import get_central_database_dsn, \
    get_node_local_database_dsn
from tools.unhandled_greenlet_exception import \
    unhandled_greenlet_exception_closure

from handoff_server.pull_server import PULLServer
from handoff_server.rep_server import REPServer
from handoff_server.handoff_requestor import HandoffRequestor
from handoff_server.handoff_manager import HandoffManager

_local_node_name = os.environ["NIMBUSIO_NODE_NAME"]
_log_path = u"%s/nimbusio_handoff_server_%s.log" % (
    os.environ["NIMBUSIO_LOG_DIR"], _local_node_name,
)
_node_names = os.environ["NIMBUSIO_NODE_NAME_SEQ"].split()
_client_tag = "handoff_server-%s" % (_local_node_name, )
_handoff_server_addresses = \
    os.environ["NIMBUSIO_HANDOFF_SERVER_ADDRESSES"].split()
_handoff_server_pipeline_address = \
    os.environ["NIMBUSIO_HANDOFF_SERVER_PIPELINE_ADDRESS"]
_central_database_pool_size = 1 
_central_pool_name = "default"
_local_database_pool_size = 1 

def _handle_sigterm(halt_event):
    halt_event.set()

def _setup(zmq_context, event_push_client, halt_event):
    """
    set up program, return a list of greenlet to termiante in teardown
    """
    local_node_index = _node_names.index(_local_node_name)
    assert local_node_index != -1
    active_group = Group()

    interaction_pool = DBInteractionPool(get_central_database_dsn(), 
                                         pool_name=_central_pool_name,
                                         pool_size=_central_database_pool_size, 
                                         do_log=True)

    interaction_pool.add_pool(dsn=get_node_local_database_dsn(), 
                              pool_name=_local_node_name,
                              pool_size=_local_database_pool_size) 

    query = """select id, name from nimbusio_central.node 
               where cluster_id = 
                   (select cluster_id from nimbusio_central.node
                    where name = %s)"""

    async_result = \
        interaction_pool.run(interaction=query, 
                             interaction_args=[_local_node_name, ],
                             pool=_central_pool_name) 
    result_list = async_result.get()

    assert len(result_list) == len(_node_names)

    # we assume node-name will never be the same as node-id
    node_dict = dict()
    for entry in result_list:
        node_dict[entry["id"]] = entry["name"]
        node_dict[entry["name"]] = entry["id"]

    local_node_id = node_dict[_local_node_name]

    incoming_reply_queue = Queue()

    pull_server = PULLServer(zmq_context, 
                             _handoff_server_pipeline_address,
                             incoming_reply_queue,
                             halt_event)
    pull_server.link_exception(
        unhandled_greenlet_exception_closure(event_push_client))
    pull_server.start()
    active_group.add(pull_server)

    reply_dispatcher = ReplyDispatcher(zmq_context,
                                       interaction_pool,
                                       event_push_client,
                                       incoming_reply_queue,
                                       node_dict,
                                       _client_tag,
                                       _handoff_server_pipeline_address,
                                       halt_event)
    reply_dispatcher.link_exception(
        unhandled_greenlet_exception_closure(event_push_client))
    reply_dispatcher.start()
    active_group.add(reply_dispatcher)

    incoming_request_queue = Queue()

    rep_server = REPServer(zmq_context, 
                           _handoff_server_addresses[local_node_index],
                           incoming_request_queue,
                           halt_event)
    rep_server.link_exception(
        unhandled_greenlet_exception_closure(event_push_client))
    rep_server.start()
    active_group.add(rep_server)

    remote_handoff_server_addresses = list()
    for index, address in enumerate(_handoff_server_addresses):
        if index != local_node_index:
            remote_handoff_server_addresses.append(address)
            
    push_client_dict = dict()
    request_dispatcher = RequestDispatcher(zmq_context,
                                           interaction_pool, 
                                           event_push_client,
                                           incoming_request_queue, 
                                           push_client_dict,
                                           halt_event)
    request_dispatcher.link_exception(
        unhandled_greenlet_exception_closure(event_push_client))
    request_dispatcher.start()
    active_group.add(request_dispatcher)

    handoff_requestor = HandoffRequestor(zmq_context, 
                                         remote_handoff_server_addresses,
                                         local_node_id,
                                         _client_tag,
                                         _handoff_server_pipeline_address,
                                         halt_event)
    handoff_requestor.link_exception(
        unhandled_greenlet_exception_closure(event_push_client))
    handoff_requestor.start()
    active_group.add(handoff_requestor)
    
    socket_greenlets = [rep_server, pull_server, reply_dispatcher, ]

    return active_group, socket_greenlets, push_client_dict

def main():
    """
    main processing module
    """
    initialize_logging(_log_path)
    log = logging.getLogger("main")
    log.info("program initializing")

    halt_event = Event()
    gevent.signal(signal.SIGTERM, _handle_sigterm, halt_event)

    zmq_context = zmq.Context()

    # do the event push client first, because we may need to
    # push an execption event from setup
    event_push_client = EventPushClient(zmq_context, "handoff_server")

    try:
        active_group, socket_greenlets, push_client_dict = \
            _setup(zmq_context, event_push_client, halt_event)
    except Exception:
        log.exception("exception during setup")
        return 1
        
    log.info("program started")
    event_push_client.info("program-start", "handoff_server starts")

    # wait here while the servers process messages
    halt_event.wait()

    log.info("halt_event set, program terminating")

    # zmq_contexct_term() will sit forever unless we close every socket
    for socket_greenlet in socket_greenlets:
        socket_greenlet.join()

    active_group.kill()
    active_group.join(timeout=3.0)

    for push_client in push_client_dict.values():
        push_client.close()

    event_push_client.close()
    zmq_context.term()

    log.info("program terminates normally")
    return 0

if __name__ == "__main__":
    sys.exit(main())


