# -*- coding: utf-8 -*-
"""
archiver
"""

_create_temp_table = """
drop table if exists nimbusio_collectable_segments;
create temp table nimbusio_collectable_segments (
    id int8 not null
);
"""

_create_temp_table_index ="""
create index nimbusio_collectable_segments_idx 
on nimbusio_collectable_segments (id);
"""

# in PostgreSQL 9.0 it has to be done as two queries */
_archive_segment_rows_9_0 = """
insert into segment_archived 
    select * from segment where exists (
        select 1 from nimbusio_collectable_segments ncs 
        where ncs.id=segment.id);
delete from segment where exists (
    select 1 from nimbusio_collectable_segments ncs 
    where ncs.id=segment.id);
"""

# in PostgreSQL 9.1 it can be done as a single query with a Writable Common
# Table Expression
_archive_segment_rows_9_1 = """
with deleted_rows as (
    delete from segment 
    where exists (select 1 
                  from nimbusio_collectable_segments ncs 
                  where ncs.id=segment.id) 
    returning *
)
insert into segment_archived select * from deleted_rows;
"""

_delete_segment_sequences = """
delete from segment_sequence where exists (
    select 1 from nimbusio_collectable_segments ncs
    where ncs.id=segment_sequence.segment_id);
"""

_archive_old_tombstones = """
insert into segment_archived
    select * from segment where status = 'T'
    and age(timestamp) > %(max_node_offline_time)s::interval;

delete from segment where status = 'T' 
and age(timestamp) > %(max_node_offline_time)s::interval;
"""

def _archive_collectable_segment_rows(
    connection, collectable_segment_ids, max_node_offline_time
):
    connection.execute(_create_temp_table, [])

    # bulk load the temp table, before the index is created
    collectable_segment_ids.seek(0)
    cursor = connection._connection.cursor()
    cursor.copy_from(collectable_segment_ids, 
                     "nimbusio_collectable_segments")
    cursor.close()

    # create an index on the temp table
    connection.execute(_create_temp_table_index, [])

    # delete the collected rows from the segment table
    # archiving them to segment_archive
    # TODO: we could parse "select version()"
    connection.execute(_archive_segment_rows_9_0, [])

    # now delete from segment_sequences (they are not archived)
    connection.execute(_delete_segment_sequences, [])

    # finally clean out old tombstones
    connection.execute(_archive_old_tombstones, 
                       {"max_node_offline_time" : max_node_offline_time, })

def archive_collectable_segment_rows(
    connection, collectable_segment_ids, max_node_offline_time
):
    """
    In a DB transaction:
    * create an unlogged single column temp table and insert the accumulated 
      segment ids
    * add an index to the temp table
    * use a writable common table expression to delete the rows from segment 
      * with an ID in the temp table, 
      * OR are a tombstone older than MAX_NODE_OFFLINE_TIME
    insert the rows from the table expression into segment_archived
    """
    connection.execute("begin")
    try:
        _archive_collectable_segment_rows(connection, 
                                          collectable_segment_ids,
                                          max_node_offline_time)
    except Exception:
        connection.rollback()
        raise
    else:
        connection.commit()

