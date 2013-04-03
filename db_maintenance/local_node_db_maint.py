#!/usr/bin/env python2.7

"""
gc:
 - connect to central db and get set of versioned collections, and the deleted
   time for deleted collections.
 - insert this into the node db as a temp table
 - move garbage rows into the garbage_segment_conjoined_recent table.
   - select the garbage rows into a temp table using the unoptimized garbage
     views, referencing results w/ the temp tables above to know if a
     collection is versioned.)
   - add an index
   - update the temp table for rows to have and end time of when the collection
     was deleted, if it was deleted earlier than the existing end time.
   - insert into the temp table additional garbage_segment_conjoined rows that
     are from deleted collections but whos IDs aren't already in the temp
     table.  (i.e. rows who became garbage because the collection was deleted,
     not because of regular reasons for becoming garbage.)
   - for rows that are already known to be garbage, correct any end times that
     now calculate differently by updating those it the
     garbage_segment_conjoined_recent table.  (Note that for this to work
     correctly, it must be impossible to make a versioned collected become
     unversioned again.)
   - summerize the new garbage rows into a gc history table, along w/ number of
     corrections from previous step.
   - insert the new rows garbage rows from temp table into
     garbage_segment_conjoined_recent
   - delete those IDs existing in the temp table from segment
   - delete rows from segment_sequence referencing IDs existing in the temp
     table

billable calculation: (calculates billable stats per collection per month)
 - calculate billing columns per collection for this month and months that are
   up to (max handoff age) interval into the past, and as recent as (precice
   cutoff time so nodes have a chance of being close to consistent.)
 - insert rows into billing calculation history table w/ current date as calculated date

maintenance:
 - move all the archives from garbage_segment_conjoined_recent to old that are
   beyond handoff age
 - move all the archives from garbage_segment_conjoined_old to ancient that are
   from a month beyond the billing recalculation age.
"""

import logging
import zmq
import os
import sys

from tools.standard_logging import initialize_logging
from tools.event_push_client import EventPushClient, unhandled_exception_topic
from tools.database_connection import \
    get_central_connection, get_node_local_connection

_local_node_name = os.environ["NIMBUSIO_NODE_NAME"]
_log_path = "{0}/nimbusio_db_maintenance_{1}.log".format(
    os.environ["NIMBUSIO_LOG_DIR"], _local_node_name,
)

_gc_queries = [
"""
CREATE temp table _tmp_new_garbage AS 
SELECT segment_id,
       collection_id,
       key,
       status,
       unified_id,
       timestamp,
       segment_num,
       conjoined_part,
       file_size,
       file_adler32,
       file_hash,
       file_tombstone_unified_id,
       source_node_id,
       handoff_node_id,
       conjoined_id,
       conjoined_create_timestamp,
       conjoined_abort_timestamp,
       conjoined_complete_timestamp,
       conjoined_delete_timestamp,
       combined_size,
       combined_hash,
       current_timestamp as collected_time,
       CASE WHEN collection_deletion_time is not null 
              THEN collection_deletion_time
            WHEN (collection_is_versioned=true 
                  and versioned_end_time is not null)
              THEN versioned_end_time
            WHEN (collection_is_versioned is null 
                  and unversioned_end_time is not null)
              THEN unversioned_end_time
       END as collected_end_time,
       null::int8 as collected_by_unified_id
  FROM nimbusio_node.gc_archive_batches_with_end_time gcabwet
  LEFT JOIN _tmp_versioned_collections 
    ON (gcabwet.collection_id = _tmp_versioned_collections.collection_id)
  LEFT JOIN _tmp_collection_del_times
    ON (gcabwet.collection_id = _tmp_collection_del_times.collection_id)
 WHERE ((collection_deletion_time is not null) or
        (collection_is_versioned=true and versioned_end_time is not null) or
        (collection_is_versioned is null and unversioned_end_time is not null));
""",

"""
CREATE INDEX _tmp_new_garbage_segment_idx on _tmp_new_garbage (segment_id)
""",

"""
CREATE INDEX _tmp_new_garbage_conjoined_id on _tmp_new_garbage (conjoined_id)
""",

"""
DELETE FROM segment where exists (
    SELECT 1
      FROM _tmp_new_garbage
     WHERE segment_id=segment.id)
""",

"""
DELETE FROM conjoined where exists (
    SELECT 1
      FROM _tmp_new_garbage
     WHERE conjoined_id=conjoined.id);
""",

]

def get_versioned_collection_ids(central_conn):
    """
    """
    sql = "select id from collection where versioning=true"
    rows = central_conn.fetch_all_rows(sql)
    return set([r[0] for r in rows])

def get_collection_deletion_times(central_conn):
    """
    """

    sql = """
SELECT id, deletion_time::text
  FROM collection 
 WHERE deletion_time is not null
    """.strip()

    deleted_collection_times = dict()

    rows = central_conn.fetch_all_rows(sql)
    deleted_collection_times.update((r[0], r[1], ) for r in rows)
    return deleted_collection_times

def populate_collection_info_temp_tables(conn, versioned, deletion_times):
    """
    """

    setup_queries = [
"""
create temp table _tmp_versioned_collections ( 
    collection_id int4 not null,
    collection_is_versioned bool not null default true
)
""".strip(),
"""
create temp table _tmp_collection_del_times ( 
    collection_id int4 not null,
    collection_deletion_time timestamp not null
)
""".strip(),
    ]

    index_queries = [
"""
create index _tmp_versioned_collections_idx
    on _tmp_versioned_collections_idx (collection_id)
""".strip(),
"""
create index _tmp_collection_del_times_idx
    on _tmp_collection_del_times (collection_id)
""".strip(),
    ]

    for query in setup_queries:
        conn.execute(query)
    
    sql = "insert into _tmp_versioned_collections (collection_id) values (%s)"
    for collection_id in sorted(list(versioned)):
        conn.execute(sql, [collection_id])

    for collection_id, deletion_time in deletion_times.iteritems():
        conn.execute("insert into _tmp_collection_del_times values (%s, %s)",
            [collection_id, deletion_time])

    for query in index_queries:
        conn.execute(query)


def gc_pass():
    """
    """

    central_conn = get_central_connection()
    central_conn.set_logger(logging.getLogger("central_db"))
    local_conn = get_node_local_connection()   
    local_conn.set_logger(logging.getLogger("local_db"))
    versioned_collections = get_versioned_collection_ids(central_conn)
    collection_deletion_times = get_collection_deletion_times(central_conn)
    central_conn.close()
    local_conn.begin_transaction()
    populate_collection_info_temp_tables(
        local_conn, versioned_collections, collection_deletion_times)
    for query in _gc_queries:
        local_conn.execute(query)

def main():

    initialize_logging(_log_path)
    log = logging.getLogger("main")
    log.info("program starts")
    zmq_context =  zmq.Context()

    event_push_client = EventPushClient(zmq_context, "db_maintenance")
    event_push_client.info("program-start", "db_maintenance starts")

    try:
        gc_pass()
    except KeyboardInterrupt:
        log.warn("keyboard interrupt")
        return 1
    except Exception as instance:
        log.exception(str(instance))
        event_push_client.exception(
            unhandled_exception_topic,
            str(instance),
            exctype=instance.__class__.__name__
        )
        return 1

    log.info("program terminates normally")
    return 0

if __name__ == '__main__':
    sys.exit(main())
