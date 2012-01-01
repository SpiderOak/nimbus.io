Administrator's Guide
=======================================================

Contents:

.. toctree::
    :maxdepth: 10

To cloud or not to cloud?
^^^^^^^^^^^^^^^^^^^^^^^^^

Sts section provides background and recommendations for when to chose
running your own Nimbus.io cloud locally vs. using the commercially
available Nimbus.io cloud storage service.

Much like the SR71 Blackbird does not operate efficiently at low speeds in
favor of elegant behavior at extreme speed, Nimbus.io is efficient for
storing vast amounts of data. [#]_ 

Production Nimubs.io storage clusters involve groups of 10 computers acting
redundantly with data stripped across all of them.  Much like RAID6
stripes data across many disks allowing for some disks to fail, Nimbus.io
stripes data across many whole machines.

Nimbus.io eats storage nodes 10 at a time.  Generally each of the 10 nodes
has dozens of TB of internal storage capacity.  When the first storage
cluster approaches capacity, it is not possible to incrementally expand by
buying a few more storage nodes; you must instead buy a second complete
storage cluster. [#]_ 

At current market rates, a complete set of hardware for operating a
Nimbus.io 10 node storage cluster might represent a one time cost of
$90,000 providing a storage capacity of 320 TB.  It might also cost $1000
per month in electricity, and some small amount for physical
administration.  

At Amazon S3 storage rates, 320 TB of data would cost almost $37,000 every
month, or $19200 for The Nimbus.io cloud storage service.  Clearly, with
data approaching this large a capacity, rolling your own cloud has the
opportunity for substantial cost savings.  However, for more typically
sized collections of data, we recommend chosing a cloud storage service.

Operating your own Nimbus.io infrastructure might also make economic sense
for other reasons other than storage costs, such as bandwidth costs, or the
cost of local computing infrastructure vs. the higher cost of cloud
computing infrastructure. 

Storage Network and Cluster Architecture  
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A Nimbus.io service needs a tiny central database, and then 1 or more
storage clusters.  The central database is independent of any storage
cluster, and contains customer account information, and the mapping between
collections and storage clusters (i.e.  a table of which storage cluster
each collection is serviced by.)  Even if you have millions of customers
with thousands of collections, this central database will be small enough
to easily scale vertically.  The actual storage clusters scale
horizontally.  

The central database should be arranged redundantly in a high availability
setup.  There are a variety of ways to accomplish this.  We use PostgreSQL
9.1's built in synchronous replication.  Another common approach is to use
Linux DRBD and Pacemaker to synchronously replicate a set of VMs across
multiple physical servers.  Creating a highly avaliable PostgreSQL setup
for the central database is outside the scope of this document, as there
are many excellent guides available.

Storage clusters are groups of 10 homogeneous machines acting as a team.
The hardware and the role of each machine is identical (i.e. there are no
special machines or single points of failure within a storage cluster.)
When you need more storage, you add more storage clusters.  Migration
processes handle moving collections among storage clusters.

Storage clusters may be located across in any number of data centers.
Dynamic DNS maps requests for each collection (which has a unique hostname)
to network addresses within the data center that service it.  For example,
the key "giraffe" in collection "kansas-city-zoo" might have the URL of:
`https://kansas-city-zoo.nimbus.io/data/giraffe <https://kansas-city-zoo.nimbus.io/data/giraffe>`_. DNS resolution for
kansas-city-zoo.nimbus.io would resolve to an IP in the data center hosting
the storage cluster that services the kansas-city-zoo collection.

We generally allocate one public IP per storage cluster, but you can
survive with as little as one IP per data center at the cost of some added
intelligence in the load balancers.  

Load balancers at the data center direct incoming traffic to any one of the
cluster's storage nodes.

If you can find a few data centers with site-to-site links (such as a metro
area fiber ring), consider locating the 10 storage nodes in a cluster
across several sites.  For example, two nodes in each of five
interconnected sites.  You can then achieve geographical redundancy.
Bandwidth for site-to-site links are often dramatically less expensive than
Internet bandwidth.

These details of your data centers, clusters, and nodes are configured
within the data_center, storage_cluster, and storage_node tables in the
central database.

Hardware Selection
^^^^^^^^^^^^^^^^^^

Hardware selection should be driven mostly by the pattern of data access
you need your clusters to support.  Note that you can have multiple
clusters and assign collections to different clusters depending on their
relative performance needs.  The colder your data, or the more sequential
your access patterns, the greater storage density you can safely use.  If
you need to support highly concurrent and low latency read access, you need
to dedicate more and faster drives within each storage node to caching.

For the Nimbus.io commercial storage service, a typical storage node
includes 36 SATA drives, a high end RAID controller, a decent SSD, 32g of
RAM and 4 CPUs.  The spinning drives are arranged into a few RAID6 storage
volumes, with a few disks left over as hot spares spool or cache volumes.
The SSD contains metadata only (i.e.  the node's own local database.)  We
typically use the lower power "green" drives.

For best performance, we recommend at least two sets of disks: one small
volume to receive the spool of sequential incoming writes, and other
volumes to store the bulk of the data and service reads.

It's critical to configure the raid controller to work in write-through
mode (not caching writes.)  Expect data corruption if your rack loses power
and you have not done this.  We don't trust battery backup units. [#]_

Feel free to contact us if you would like advise on specific hardware
selection.  You may also purchase Nimbus.io storage clusters directly from
SpiderOak.  We buy enough of them that we can charge you a very modest
markup and still underprice other vendors.  We can drop ship a storage
cluster in a rack ready to roll into place and run immediately. 

OS Configuration
^^^^^^^^^^^^^^^^

The commercial Nimbus.io storage service operates on Linux Ubuntu LTS
releases, but any Unixish operating system should work for running your own
cluster.  We expect Windows might even work with minor effort, but it has
not been tested.

On Linux, we recommend the ext4 file system for storage volumes, and ext2
for incoming write volumes (which are basically journals themselves, making
the journaling from ext4 redundant.)  Despite its popularity, we recommend
against XFS for large storage volumes.

File systems should be mounted with noatime. Set the read ahead for block
devices to high values (blockdev --setra); experiment with 4096, 8192,
16384 or even 32768.  Use the deadline IO scheduler if you need to limit
read latency, otherwise stick with CFQ for higher overall throughput.  

Regrettably, we also must recommend a policy of rebooting Linux machines
after 200 days of uptime, because of this Kernel bug: TODO

The PostgreSQL documentation's chapter on data write to disk reliability
should be considered required reading.  Everything discussed there applies
equally to reliable writes to persistent hardware with Nimbus.io.  
`http://www.postgresql.org/docs/9.1/static/wal-reliability.html <http://www.postgresql.org/docs/9.1/static/wal-reliability.html>`_

Webserver and Load Balancer Configuration
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

We use IPVS and Haproxy talking to Nginx. Nginx runs on each storage node,
handles the SSL, and does HTTP forwarding to the application level web
server process.

Stored Node Database Configuration
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Each storage node has a PostgreSQL database instance that is local to that
node.  This is because filesystems are notoriously bad at storing large
numbers of small files.  So many items are aggregated together and stored
in a fewer number of larger files.  PostgreSQL keeps track of which data is
where.  PostgreSQL should be configured for trust 

Depending on the average size of keys stored within your cluster, there
will either be greater pressure on the file system (for disk IO) or the
database (for retrieving meta data.)  You can therefore adjust the
PostgreSQL performance related parameters accordingly to dedicate more
machine resources to one task or another.  Particularly, you can increase
or decrease how much memory PostgreSQL dedicates to shared buffers for
things like query caching.

The recommended configuration is to run the PostgreSQL instance with its
data files residing on a SSD.  Then configure PostgreSQL log shipping to
archive database changes to one of the storage volumes, or send them via
the REST API to a different Nimbus.io storage cluster, so that we can
recover the majority of the database when the SSD dies (and let
anti-entropy handle the remainder.)


Cache Layer Configuration
^^^^^^^^^^^^^^^^^^^^^^^^^

Latency of read requests to Nimbus.io can be made arbitrarily small by
devoting sufficient additional resources to caching.

Nimbus.io is intended to integrate with Varnish and other frontend content
caching tools.  TODO describe how to do this. Note about the streaming
issue.

Supported Platforms
^^^^^^^^^^^^^^^^^^^

Installing Nimbus.io
^^^^^^^^^^^^^^^^^^^^

To just experiment with Nimbus.io, the easiest way is to run the cluster
simulator.(TODO: link to cluster simulator.)  This and the following
section describe how to install Nimbus.io on for serious development or
production uses.

First, checkout the latest version of the source code:

git clone `https://nimbus.io/dev/git/nimbus.io/ <https://nimbus.io/dev/git/nimbus.io/>`_


Install all the needed libraries and other dependencies.  There are well
commented shell scripts to guide you through this for some operating
systems in the scripts/install folder within the source code.

In general, Nimbus.io depends on all of the following:

* Python 2.6+
* PostgreSQL 9.1+
* ZeroMQ 2.1.10+
* Python libraries: cython, gevent, gevent-zeromq, webob, zfec

To test the installation of your dependencies, try running the cluster
simulator (TODO link to running cluster simulator in developers guide.)

Configuring Nimbus.io for your site
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

TODO write all these subsections

Chose a domain name. Get SSL certificates if desired. 
+++++++++++++++++++++++++++++++++++++++++++++++++++++

Allocate External and Internal IP addresses.
++++++++++++++++++++++++++++++++++++++++++++

Create the central database and apply the schema
++++++++++++++++++++++++++++++++++++++++++++++++

Insert records for data centers, storage clusters, storage nodes
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

Setup dynamic DNS
+++++++++++++++++

For each storage node:
++++++++++++++++++++++

Create and apply schemas for the local database
-----------------------------------------------

Create the node's config script
-------------------------------

Setup Runit And Run Scripts
---------------------------

Connecting Nimbus.io clients to your own site
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Set NIMBUSIO_SIM_HOSTNAME variable and SSL variables as appropriate
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

Management and Monitoring
^^^^^^^^^^^^^^^^^^^^^^^^^

We use ganglia to graph system health and trends over time, RRDTool style.
The nimbus.io services emit some stats (via the statgrabber library) that
ganglia will graph along side the typical system status such as CPU, IO, and
memory usage.

We use Nagios for monitoring the health of the central DB and each storage
node, with alerts escalating as needed.  In addition to the stock tests for
Nagios, there are functional tests that continuously cycle through testing
the full set of operations possible on a test collection that has been
assigned to each production storage cluster.

We recommend installing atop on each node to keep historical records of
busy-percentage by storage volume.  Use iotop or vmstat to track disk usage
by process.  This combined with atop can give you good information on how to
tweak your cluster hardware arrangements if you need to improve performance.

For a large Nimbus.io service with many collections and many clusters, the
biggest management challenge is balancing space usage across the many storage
clusters.  There are some home grown tools for this with goofy names like
space cadet, space colony, and space ship which we'll work on releasing as
free and open source software.

Recovery from Node Failure
^^^^^^^^^^^^^^^^^^^^^^^^^^

Recovering from the total hardware loss of any storage node in a cluster is
a simple two step process: 

#. Replace the storage node with a new identically configured node 

#. Actually, there is no step 2.  The anti entropy process will automatically take it from here. 

In practice, loss of the entire node is very rare.  Even loss of a single
RAID6 storage volume is rare, but with enough volumes it will happen
eventually.  The most common scenario is that  component of a machine fails,
and the rest is salvageable.  This allows you to bring the storage cluster
back to full health with a shorter rebuild time for the anti entropy service
to restore replication level for lost data.

The general procedure for failure from partial node loss is:

#. Take the Nimbus.io services offline (this typically happens automatically with severe failure.)  The other nodes will continue to operate normally, creating hinted-handoffs for data destined for the offline node. 

#. If the node's local database was lost, restore from the most recent dump and then apply any archived log files to bring it close to current.  Even an old backup will reduce the amount of work anti entropy must do.  If the node local database cannot be restored at all, treat the situation as a total machine failure, re-initializing all the storage volumes, etc.

#. Replace the failed component(s).  This may mean recreating and re-initializing one or more storage volumes. 

#. For any storage volume that was lost, delete all entries in the node local database for value files located on that storage volume (use a cascade delete, also deleting any keys that reference the lost value files.) 

#. Bring the node's Nimbus.io services online, and allow anti entropy to begin. 

.. rubric:: Footnotes:

.. [#] TODO: Link to relevant nerdy information about the SR71 and how it leaks fuel on the ground.

.. [#] Although there are some ways you can incrementally extend a single cluster, such as buying 10 machines, but only 1/3 of the hard drives to fill them, and adding more drives over time as needed.

.. [#] There are some advanced and durable alternatives to this if you absolutely need the added performance of write caching.

`nimbus.io <https://nimbus.io/>`_ 

