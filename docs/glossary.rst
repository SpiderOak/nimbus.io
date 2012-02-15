
********
Glossary
********

A list of all terms commonly used when discussing or developing Nimbus.io. This
is organized in a somewhat top down order to minimize the number of forward
pointing references. 

Some of the deeper terms within the Nimbus.io code base are omitted.

.. glossary::

A list of all terms commonly used when discussing or developing Nimbus.io.
This is organized in a somewhat top down order to minimize the number of
forward pointing references.  

Some of the deeper terms within the Nimbus.io code base are omitted.

    Nimbus.io
        A commercial storage service provided by SpiderOak **AND ALSO** a free
        software project for the highly available and highly durable server
        side storage storage software used by SpiderOak to provide the
        Nimbus.io storage service.  Other organizations may freely reuse the
        Nimbus.io storage software to operate their own storage service under
        the terms of the license.    

    Nimbus.io Service Provider
        Any organization running the Nimbus.io software to provide a storage
        service.  This includes the Nimbus.io commerical storage service
        offered by SpiderOak.

    Nimbus.io Service Domain
        The domain used by a Nimbus.io service provider.  For the commericial
        service operated by SpiderOak, this is "nimbus.io."  This forms the
        basis of URLs for buckets and keys.  For example, the host name for the
        collection "mycollection" is mycollection.nimbus.io.  

        This is configured within the Nimbus.io software and client libraries
        with the environment variables::

            NIMBUS_IO_SERVICE_DOMAIN (default nimbus.io)
            NIMBUS_IO_SERVICE_PORT (default 443)
            NIMBUS_IO_SERVICE_SSL (default 1, change to 0 to disable SSL)

        Additionally, to make life easier for developers running cluster
        simulators locally (and avoid unnecessary /etc/hosts editing), we have
        arranged for the domain "sim.nimbus.io" and all subdomains to resolve
        to 127.0.0.1.  For example, "abc.sim.nimbus.io" returns 127.0.0.1.  

    Customer
        A user of Nimbus.io via a Nimbus.io service provider.  The customer may
        have zero or more :term:`Collection` of data.  Customers have a record
        in the customer table in the :term:`Central Database`.

    Collection
        A defined container for storage within a customer's account.  Amazon S3
        and Google call these "buckets."  Collection names are globally unique
        across all customers for a Nimbus.io service provider.  There are some
        provisions to make it easy for customers to generate new collection
        names without worry of collision with existing collections.
        Collections have a record in the collection table in the Central
        Database.

    Central Database
        A central PostgreSQL database operated by a Nimbus.io service provider.
        This is the only component within Nimbus.io that is not horizontally
        scalable.  It's designed to be tiny such that horizontal scaling is
        unnecessary.  This central database contains only customer, billing,
        and collection information such that the size of the database may
        remain small and easily vertically scaled.

        Specifically, the central database contains the list of collections for
        each customer.  Also the central database records which data centers
        and storage clusters each collection is serviced by.

        It's recommended that the central database be arranged in a high
        availability configuration with PostgreSQL 9.1 Synchronous Replication.

    Dynamic DNS
        For Nimbus IO Service providers with multiple data centers, Dynamic DNS
        is used to map collection hostnames to the data centers that operate
        them.   This information is fed into a dynamic DNS system from the
        central database.  Nimbus.io service providers without multiple data
        centers do not need Dynamic DNS and can use simple DNS wildcards.

    Data Center
        A physical site operated by a NSP, and hosting one or more Storage
        Clusters.  Data centers are included in the configuration in the
        central database.  

    Storage Cluster
        A group of 10 Storage Nodes working together to store collections of
        Nimbus.io data.  In Nimbus.io, storage capacity is always deployed in
        clusters of 10 units.

    Storage Node
        A distinct hardware unit participating in a Storage Cluster.  All
        Storage Nodes within a cluster are homogenous in their hardware and
        software configuration.  A Storage Node should be entirely self
        contained, such that multiple storage nodes will not fail for the same
        causes.  (For example, multiple storage nodes should use the same
        shared SAN for storage.)

    Storage Volume
        A particular set of disks within a Storage Node, grouped as a volume
        and file system.  The Nimbus.io software is intended to use an
        efficient multi volume IO strategy.  Different storage volumes may
        serve different purposes, such as caching, journaling, hosting the Node
        Database, or general storage.  

    Node Database
        A database maintained on each Storage Node.  It contains information
        about the keys and values stored on the node (including Tombstones and
        Handoffs.)  Basically, the node database provides lookup information
        for the binary data that is actually stored in the file system, in
        value files.

    Key
        Within a collection, items are stored by keys.  These are Unicode
        strings of not more than 1024 characters used to Archive and Retrieve
        key/value pairs.

    Value
        The payload or binary data stored in a collection for a particular key.

    Archive
        The act of storing data in a collection under a particular key (i.e.
        storing a new key and value.)  Archiving happens via a POST request to
        the REST API.

    Retrieve
        The opposite of archive. To retrieve data from a Nimbus.io service by
        key.  Retrieve happens via a GET request to the REST API.

    Conjoined Archive
        Analogous to what Amazon S3 calls "multi part uploads."  This is a way
        to upload a very large file using smaller pieces, such that the
        transfer can be resumed if interrupted.

    Tombstone
        A delete marker.  Whenever a key is deleted from a collection, a
        tombstone is written, marking the key as having been deleted.  The
        tombstone stays is retained for some time allowing handoffs to
        propagate.  This is a common technique in distributed systems and
        "eventually consistent" databases.  Deleted keys are no longer provided
        in response to retrieve requests, and are later collected during
        garbage collection.

    Handoff
        Part of Nimbus.io's High Availability and Redundancy strategy.  When
        one or more of the nodes in a storage cluster is offline, new writes to
        the cluster may continue.  The data that would have been sent to the
        offline node is instead "handed off" to two other nodes within the
        cluster.  When the offline node returns to service, handoffs will be
        passed to it.  This creates an "eventually consistent" environment.
        Both normal archives and Tombstones are handed off.

    Replication
        A durability strategy often used by distributed storage systems,
        including Nimbus.io.  This involves keeping multiple copies of objects
        (replicating them) some number of times.  Within Nimbus.io, the
        replication strategy is used for only Tombstones and Handoffs, and not
        general data storage.  

    Replication Level
        The number of nodes in a distributed system that must fail to cause
        data loss.  A typical replication level is 3, meaning that the system
        can survive the loss of any two distinct nodes.  Within the Nimbus.io
        storage system, a default replication factor of 3 is used but it is
        accomplished through a combination of Replication and Parity storage.
        This means that within any Storage Cluster, 2 of the 10 nodes can fail
        without resulting data loss.  

        Note that this is in addition to whatever redundancy strategies are
        contained within the storage hardware of each Storage Node (RAID6 or
        RAID1, for example.)

    Parity
        Parity is a space efficient alternative to replication.  Within
        Nimbus.io, parity is used as the primary redundancy strategy.  Item
        storage is striped with parity across all nodes in a storage cluster,
        instead of replicated to a few nodes.  Both systems tolerate the same
        amount of failure without data loss.  Replication creates a 200% size
        overhead (2 extra copies) while parity with the same replication factor
        creates only 25% overhead.

        Parity the strategy used by RAID5 and RAID6 to protect against one or
        two disk failures within a disk arry.  It is also used by optical disk
        formats to preserve data integrity even in the presence of surface
        defects like scratching on the disk.  

        The basic idea is that a data is encoded into several separate parts
        each containing some additional information.  This extra information
        allows decoding the original data even if some of the parts are lost.
        It does not matter which parts are lost; if enough parts remain, the
        data can be decoded.  

    Nimbus.io web server
        The application level web server serving the REST API.  In production
        configurations this server is not directly addressable by end users,
        but generally runs behind a general web server software such as Nginx,
        and perhaps also a caching layer.

        Each Nimbus.io Storage Node within a Storage Cluster operates a Nimbus
        Web Server, and each node is capable of servicing REST API requests on
        behalf of the Storage Cluster.

    Sequence
        When an :term:`Archive` request in being handled by the
        :term:`Nimbus.io Web Server` the request is broken up into fixed size
        pieces called sequences.  The default sequence size is 10 megabytes.

        Each sequence is is encoded with parity, creating redundancy across 10
        distinct :term`Zfec Share`s.  These are sent to each of the 10
        :term:`Storage Nodes` in the :term:`Storage Cluster`, directly or via
        :term:`Handoff` for offline node(s).  They are stored as :term:`Segment
        Sequences`.

        This continues until the full length of the :term:`Value` has been
        received and stored.  The result is that each :term:`Storage Node`
        contains a :term`Segment` of the data.

    Segment
        The portion of a Key and Value pair stored on a single specific Storage
        Node, including parity overhead.  It is represented by a record in the
        :term:`Node Database` segment table.  Tombstones and Handoffs are also
        stored as Segments.  Each segment has an ID, a unified ID, a
        destination node, and a segment number ranging from 1 to 10.

    Segment-Sequence
        A record in the segment_sequence table in the :term:`node database` and
        linked to a particular segment ID.  It contains the portion of a Key
        and Value pair stored on a single specific Storage Node for a specific
        sequence of the received Value.  Segment-Sequences are numbered from 1
        to N.  A segment sequence references binary storage by within a Value
        File by size, offset, and digest.

    Value File
        A real file within the file system storing binary data for Values.
        Value files are created by :term:`Data Writer` and read by :term:`Data
        Reader`.  They are referenced by :term:`Segment-Sequence` records.

    Zfec Share
        Output from the Zfec parity library.  Given an original piece of data,
        a configurable number of shares, and a minimum number of shares needed
        to recover the data Zfec outputs a list of binary blobs encoding the
        original data with redundancy.

    Data Writer
        The Nimbus.io process on a Storage Node that the Nimbus.io Web Server
        communicates with (via ZeroMQ) to write new storage objects.  The web
        server communicates with all of the available data writers in the
        cluster to store new objects.

    Data Reader
        The Nimbus.io process on a Storage Node that the Nimbus.io Web Server
        communicates with (via ZeroMQ) to read existing storage objects.

    Garbage Collection
        The process of identifying stored objects that are no longer needed,
        and reclaiming storage space.  This is generally done as a periodic
        maintenance task.

    Anti-Entropy
        The process of automatically finding and fixing inconsistencies within
        a storage cluster.  This is generally done as a periodic maintenance
        task.  Anti entropy is also the standard method of recovery (restoring
        replication level) after Storage Node hardware failure.

    Migration
        The process of moving Collections from one Storage Cluster to another.
        Typically this is done to manage space usage and capacity.  For
        example, as a Storage Cluster becomes full, additional Storage Clusters
        are installed and some of the collections from the older Storage
        Clusters are migrated to newer Storage Clusters.

    Space Accounting
        General term for all the code and processes responsible for keeping
        track of how much space is consumed (and has historically been used) by
        each customer and collection, generally to support billing efforts.  If
        you are operating Nimbus.io internally for the needs of your own site,
        you can likely skip the space accounting work that would  be needed by
        a commercial Nimbus.io service provider.

    Virtual Collections
        Collections that are logically composed of component real Collections
        in a particular logical structure.  For example, a Virtual Collection
        might be mirrored across two or more real Collections.  A very large
        Virtual Collection might be sharded across multiple real Collections.
        This allows us to have collections larger than a single Storage
        Cluster, replication of collections across multiple data centers, or
        other quality enhancements.

        All of this is transparent to the customer.  The customer creates a
        collection like usual, and purchases a particular quality of service
        (geographic mirroring, perhaps.)  Or a collection grows to approaching
        the maximum size of a single Storage Cluster, and so we migrate its
        contents to a new Meta-Collection that is Sharded across multiple real
        Collections.

        To a customer, a virtual collection may be an invisible implementation
        detail (in the case of simply a large collection that is sharded across
        many) or it may be an upgrade a customer explicitly purchases (such as
        replication between many world sites.)
