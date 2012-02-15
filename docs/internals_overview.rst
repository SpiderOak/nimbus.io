Overview
========

This section describes the internal architecture of the Nimbus.io storage
software.  It is intended for programmers interested in participating in its
development.

This material is most approachable for first time readers if you start by just
reading the full glossary.


Tour of Database Schemas
------------------------

The central PostgreSQL database stores customers, collections, data centers,
storage clusters, and storage nodes.  A customer may have zero or more
collections.  A collection is assigned to a particular storage cluster on which
its data resides (although virtual collections allow objects in a collection to
span multiple real collections.)  A storage cluster is composed of exactly 10
storage nodes acting as a team.  A storage cluster resides in a particular data
center.

Within each storage node, there's a local PostgreSQL database.  This has
entries for value files, segments, segment sequences, and conjoined archives.
Basically, all the meta for every object is stored in this database, but the
binary blobs are stored in the file system in value files.  



Message and Component Based Architecture
----------------------------------------

Aside from the PostgreSQL databases, the major components are: web server, data
reader, data writer, handoff server, event aggregator, and event publisher.
There are also maintenance tasks: defragger, garbage collection, and anti
entropy.

The major components run on all nodes (no node is special) and communicate with
each other via ZeroMQ sockets.

Life Cycle of a PUT
-------------------

#. A client wishes to send a giraffe picture to the kansas city zoo
   collection's `kansas-city-zoo`.  This is a rather high resolution image - about
   25 meg.  

#. The client choses to store the picture in the key
   `/animals/cute/baby-giraffe.tiff`

#. The client does a DNS lookup for the collection's hostname
   `kansas-city-zoo.nimbus.io`

#. Dynamic DNS for Nimbus.io resolves the DNS query by querying the central
   database (actually a read slave.)  It joins the collection table with the
   storage_cluster table to know the public IP address the storage cluster can
   be reached at, and answers with this information.

#. The client connects to the IP address, which is answered by a nginx web
   server in the data center that the storage cluster resides.  Internal
   Dynamic DNS allows nginx to http proxy the request to the Nimbus.io web
   server running on any one node in the storage cluster that the request is
   for.  Nginx proxies the request, stripping off the SSL since the request is
   now inside the data center.

#. The web server receives the request and authenticates the headers.  The web
   server does a central database lookup (probably a read slave) to get collection
   information and verify that the request has landed on the right storage cluster

#. The web server generates a new unified_id `unid` for the request.

#. The web server begins receiving data from the client, until the data is
   large enough for the first :term:`sequence` to be available.

#. The web server encodes the first sequence into 10 segment-sequences with
   parity.  Each of these is destined for a particular storage node within the
   cluster.

#. The web server selects the destination nodes for the segment-sequences.
   Normally these are the 10 nodes of the cluster.  However, if any node is
   offline, the web server selects two alternate nodes to use for handoffs, and
   sends the messages that would have been given to the offline server to these.
   These two alternate nodes together act as a single destination.  Resilient
   zeromq sockets are established for communicating with the destinations.

#. The web server sends an `archive_key_start` message to all of the
   destination `data_writers` in the cluster.  

#. When the `data_writer` on each node receives the archive_key_start message,
   it inserts a record into the segment table
   `key=/animals/cute/baby-giraffe.tiff`, `state=Active`, unified_id=unid.  It
   writes the data into the current rolling `value file`.  It inserts a record
   into `segment_sequence` where the data is stored in the value file.

#. As more data is received by the webserver, the web server sends
   `archive_key_next` messages to the destinations, which result in more data
written into value files and more segment_sequence entries.

#. When the upload is fully received, the last segment_sequence is sent as an
   `archive_key_final` message.  At this time, each `data writer` updates the
   local database to set `state=Final` for the segment.

#. A success status is returned to the caller.
   
Other things that can happen:

#. If the client aborts the request sometime after `archive_key_start`, or
   otherwise does something that causes an error, an `archive_key_cancel` message
   is sent to data writers instead of `archive_key_final`.

#. If a storage node goes offline between the time a request begins and when it
   completes, the caller will receive a 500 level "Retry" error.  If the request
   is repeated by the client, it will likely succeed via handoff.  If the data of
   the request remains available to the web server, it maybe repeated from the
   beginning internally, without first giving the client a 500 error.


Life Cycle of a DELETE
----------------------

Similar to above, except instead of `archive_key` messages `destroy_key`
messages are sent to `data writer`.  Data writers on the nodes insert records
into their local segment tables with `state=Tombstone`.

Tombstones are garbage collected when their age is greater than the max
configured time that a node can be offline (default 1 month.)


Life Cycle of a GET
-------------------

Similar to PUT above, but the web server talks to `data reader` instead of
`data writer`.

The web server starts by querying its own local database to find the newest
segment of the key requested (i.e., the newest version.)  If none is found, a
`404 Not Found` is given to the caller.  Otherwise it retrieves the newest
version by retrieving all the segment_sequences from each node.  For any
sequence, only 8 of the original 10 segment sequences are needed (any 8 will
do.)


Life Cycle of a Handoff
-----------------------

Handoffs are created during  writes 

TODO describe handoff life cycle
