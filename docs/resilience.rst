Resilience
=======================================================

Contents:

.. toctree::
   :maxdepth: 10

Nimbus.io server processes work together over a cluster of hosts. Servers send
mesages to each other over `ZeroMQ Sockets <http://www.zeromq.org/>`_.

We have added a layer of code to recognize and recover from some transient 
communcation failures. We call it 'resilience'.

Specifically :ref:`resilient-client-label` 

connected to :ref:`resilient-server-label` 

The basic idea is that clients do not blindly keep shoving messages into a
zeromq socket's send buffer. 

We use two zeromq patterns to maintain a connection to a resilient server.

- request-reply_ for sending messages and for receiving acknowledgements
- pipeline_ for receiving the actual replies to messages

Each process maintains a single PULL_ socket for all of its resilient 
clients.

The client includes this in every message along with **_client_tag** which
uniquely identifies the client to the server

Each resilient client maintains its own DEALER_ socket **_dealer_socket**.

At startup the client sends a *handshake* message to the server. The client
is not considered connected until it gets an ack fro the handshake.

Normal workflow:

1. The client pops a message from **_send_queue**
2. The client sends the message over the DEALER_ socket
3. The client waits for and ack from the server. It does not send any
   other messages until it gets the ack.
4. When the ack is received the client repeats from step 1.
5. The actual reply from the server comes to the PULL_ socket and is
   handled outside the client

.. _ROUTER: http://api.zeromq.org/2-1:zmq-socket#toc7
.. _request-reply: http://www.zeromq.org/sandbox:dealer
.. _pipeline: http://api.zeromq.org/2-1:zmq-socket#toc11
.. _PULL: http://api.zeromq.org/2-1:zmq-socket#toc13
.. _DEALER: http://api.zeromq.org/2-1:zmq-socket#toc6

