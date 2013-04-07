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

open(
   os.path.join(os.path.dirname(__file__), 
   "gc_recent_garbage_select_query.sql"), "rb"
).read().strip(),

"""
CREATE INDEX _tmp_new_garbage_segment_idx on _tmp_new_garbage (segment_id)
""".strip(),

"""
CREATE INDEX _tmp_new_garbage_conjoined_id on _tmp_new_garbage (conjoined_id)
""",

"""
INSERT INTO nimbusio_node.garbage_segment_conjoined_recent
SELECT * FROM _tmp_new_garbage
""".strip(),

"""
DELETE FROM nimbusio_node.segment where exists (
    SELECT 1
      FROM _tmp_new_garbage
     WHERE segment_id=segment.id)
""".strip(),

"""
DELETE FROM nimbusio_node.conjoined where exists (
    SELECT 1
      FROM _tmp_new_garbage
     WHERE conjoined_id=conjoined.id);
""".strip(),

]

def get_versioned_collection_ids(central_conn):
    """
    """
    sql = """
SELECT id 
  FROM nimbusio_central.collection 
 WHERE versioning=true
    """.strip()

    rows = central_conn.fetch_all_rows(sql)
    return set([r[0] for r in rows])

def get_collection_deletion_times(central_conn):
    """
    """

    sql = """
SELECT id, deletion_time::timestamp with time zone::text
  FROM nimbusio_central.collection
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
CREATE TEMP TABLE _tmp_versioned_collections ( 
    collection_id int4 not null,
    collection_is_versioned bool not null default true
)
""".strip(),
"""
CREATE TEMP TABLE _tmp_collection_del_times ( 
    collection_id int4 not null,
    collection_deletion_time timestamp not null
)
""".strip(),
    ]

    index_queries = [
"""
CREATE INDEX _tmp_versioned_collections_idx
    on _tmp_versioned_collections (collection_id)
""".strip(),
"""
CREATE INDEX _tmp_collection_del_times_idx
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
    log = logging.getLogger("central_db")
    central_conn = get_central_connection(logger=logging.getLogger("central_db"))
    local_conn = get_node_local_connection(logger=logging.getLogger("local_db"))
    versioned_collections = get_versioned_collection_ids(central_conn)
    collection_deletion_times = get_collection_deletion_times(central_conn)
    central_conn.close()
    local_conn.begin_transaction()
    populate_collection_info_temp_tables(
        local_conn, versioned_collections, collection_deletion_times)
    for query in _gc_queries:
        rowcount = local_conn.execute(query)
        log.info("rows: %d" % ( rowcount, ))
    local_conn.commit()

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
