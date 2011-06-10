/* this schema describes the database that exists on each storage node involved
 * in DIY */

begin;

create schema diy;
set search_path to diy, public;

/* just a mostly informational table to describe the details of the whole cluster */
create table "cluster" (
    id serial primary key,
    name varchar(255) unique not null,
    node_count int4 not null default 10,
    replication_level int4 not null default 3,
    creation_time timestamp
);

create table node (
    id serial primary key,
    cluster_id int4 not null,
    node_number_in_cluster int4 not null,
    name varchar(255) unique not null,
    hostname varchar(255) not null,
    offline bool not null default false,
    creation_time timestamp
);

/* every key and every handoff are stored in the same table, so a single index
 * lookup for reads finds both the key and the handoff with the same IO, and
 * can then return each to the web server */

/* note that there are no uniqueness constraints on any of the indexes for this
 * table, so the same key could be inserted and stored multiple times (likely
 * with different timestamps, and definitely with increasing id numbers).  This
 * effectively creates "versions".

 * We could have a policy and a maintenance process that handles auto-cleaning
 * old versions periodically, thus offering offering some safety of multiple
 * versions by default */

create sequence key_id_seq;
create table key (
    name varchar(255),
    id int8 not null default nextval('diy.key_id_seq'),
    avatar_id int4 not null,
    timestamp timestamp not null,
    size int8 not null default 0,
    adler32 int4,
    hash bytea,
    user_id int4,
    group_id int4,
    permissions int4,
    tombstone bool not null default false,
    /* XXX: wasn't completely sure if this column belongs in key or
     * key_sequence. It's true that every sequence stored on the local node for
     * a given key should have the same segment_num, right?  If so, it goes
     * here. */
    segment_num int2,
    handoff_node_id int4,
    /* these constraints are written separately with distinct names to make
     * error messages more clear */
    constraint key_tombstone_zero_size check (tombstone is false or size = 0),
    constraint key_nonzero_size check (tombstone is true or size > 0),
    constraint key_adler32_not_null check
        (tombstone is true or adler32 is not null),
    constraint hash_not_null check
        (tombstone is true or hash is not null),
    constraint segment_num_not_null check
        (tombstone is true or segment_num is not null),
    constraint hash_length check (hash is null or length(hash)=16)
);




/* I need to research more about the actual implementation of multi column
 * indexes.  The goal here is to keep writes reasonably efficient even when the
 * whole index is too large to fit in RAM.  My hope is that just using a multi
 * column index means that the database can keep the parts of the index for
 * avatar IDs that are actively working in memory, and updates will be more
 * grouped into the regions of the index for which this is the case.  (I.e. it
 * has a similar effect to sharding this into a different index per avatar.) 
 */
create index key_name_idx on key("avatar_id", "name");
/* a partial index just for handoffs, so it's easy to find these records when a
 * node comes back online */
create index key_handoff_idx on key("handoff_node_id") where handoff_node_id is not null;

/* we store all the values in the diy key/value store in large, sequentially
 * written value data files.  These are pointed to by the key_sequence table to
 * find sequences and segments of stored keys (and handoffs).  

   Additional qualities of value files:

   Value files are always named based on their id in the database
   Value files are sequentially written, append only
   All inbound writes are serialized and put into the current data file
     sequentially.  The order of pending write operations maybe sorted to
     reduce fragmentation of keys, but the disk file itself is never reordered.
   A new value file is created whenever data writer starts, and whenever the
     existing value file has grown past a configurable size ("rollover")
   Value files should not be so large that they cannot be read entirely into
     memory for defrag or garbage collection (1gb seems like a good size.)
   As a value file grows, the writer process keeps track of its total size and
     hash
   During rollover, the value file is synced, closed for writing and
     updated in the database with its size and hash.
   A value file is created on disk and in the database in this order:
     Query the sequence for a new ID
     Open the file
     Add the new row to the value_file table
   Closed value files maybe garbage collected and/or defragmented and replaced:
     Identify a value_file with fragmentation or garbage via database queries
     Query the database for current key_sequence data within the old data file
     Sort the key_sequences into a defragmented order to write out
     Query the database sequence for a new value_file id
     Open new value file
     Open, Read the old value file entirely into memory, Close
     Write to the new value file in defragmented order
     Sync and close new file
     In a database transaction:
       Insert the new value_file row
       Delete all key_sequence entries that point to the old value_file
       Insert new key_sequence entries that point to the new value_file
       Assert that equal number of rows deleted and inserted
       Delete the old value_file row
     Unlink old data file
   Value files employ a disk sync strategy as they are being written.  A write 
     is not reported as being completed until the disk sync strategy says it
     should be.  Some potential strategies are:
       Just open the file with O_SYNC. That will sync every write. Simple, and 
         probably a good starting point.
       Have a sync thread that batches multiple writes by syncing a dirty file
         every unit of time, and then reports success of any write operations
         completed before sync began.  This groups multiple writes into fewer
         fsyncs.  Once every 2 seconds seems a reasonable default.

*/
create sequence value_file_id_seq;

/* this is the only table that isn't easily sharded, because data from multiple
 * avatars/keys will be in the same value files.  However, it should not be
 * necessary.  With 1gb average size value_files, a current storage node could
 * only hold 38,000 of them. */
create table value_file (
    id int4 not null primary key default nextval('diy.value_file_id_seq'),
    creation_time timestamp not null default current_timestamp,
    close_time timestamp,
    size int8,
    hash bytea, /* make it easy to verify the integrity of the whole file */
    /* this lets us quickly calculate the average item size in the value file
     * */
    /* this is the count of records appended into the file. should be updated
     * only on closing the file */
    sequence_count int4,
    /* storing min and max key ids is cheap and makes it possible to use the
     * btree indexes to find the specific key and key_sequence records stored
     * in this file via >= <= operators. */
    min_key_id int8,
    max_key_id int8,
    distinct_avatar_count int4,
    /* simple 1-d array to record the set of avatars which have data is this
     * file. If we end up sharding the key and key_sequence tables, this will
     * be very useful for maintenance. */
    avatar_ids int4[],
    garbage_size_estimate int8,
    fragmentation_estimate int8,
    last_cleanup_check_time timestamp,
    last_integrity_check_time timestamp,
    constraint hash_length check (hash is null or length(hash)=16)
);

/* this will be the largest table, as it will have 1 record for every sequence
 * of every key. */
create table key_sequence (
    avatar_id int4 not null,
    key_id int8 not null,
    value_file_id int4 not null,
    sequence_num int4 not null,
    value_file_offset int8 not null,
    size int4 not null,
    hash bytea not null,
    adler32 int4 not null,
    constraint hash_length check (hash is null or length(hash)=16)
);
/* again, need more research about the multi column index. it maybe better just
 * to drop the avatar_id column here have the single index. not sure yet. */
create index key_sequence_key_id_idx on key_sequence (avatar_id, key_id);

rollback;
