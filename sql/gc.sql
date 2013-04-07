/*
Implements a series of views that make the garbage status of segments known,
adding columns unversioned_end_time and versioned_end_time identifying when a
segment would cease to be visible (i.e. is eligible for garbage collection.) in
an unversioned  and versioned collection respectively.  The end times in
combination with begin times form a basis for billing in terms of byte-seconds
(or byte-hours.)

These are the same queries as implemented in sql/gc.py, but written as a series
of smaller views instead of nested subselects.  This form is much more readable
and easy to understand, and was written first.  However, at least as of
PostgreSQL 9.2, the query optimizer cannot see through selects across the
multiple views to do row elimination early, and therefore even a select that
theoretically could operate by index scans ends up doing a full table scan
every time.  

So these views are only really usable when we're doing a full garbage
collection pass anyway, or to check other results against.

*/

begin;
set search_path to nimbusio_node, public;
drop view gc_archive_batches_with_end_time;
drop view gc_archive_batches;
drop view gc_archive;
drop view archive;

create or replace view archive as 
select 
    segment.id as segment_id,
    segment.collection_id,
    segment.key,
    segment.status,
    segment.unified_id,
    segment.timestamp,
    segment.segment_num,
    segment.conjoined_part,
    segment.file_size,
    segment.file_adler32,
    segment.file_hash,
    segment.file_tombstone_unified_id,
    segment.source_node_id,
    segment.handoff_node_id,
    conjoined.id as conjoined_id,
    conjoined.create_timestamp as conjoined_create_timestamp,
    conjoined.abort_timestamp as conjoined_abort_timestamp,
    conjoined.complete_timestamp as conjoined_complete_timestamp,
    conjoined.delete_timestamp as conjoined_delete_timestamp,
    conjoined.combined_size as conjoined_combined_size,
    conjoined.combined_hash as conjoined_combined_hash
    /*  these are redundant 
    conjoined.collection_id,
    conjoined.key,
    conjoined.unified_id,
    conjoined.handoff_node_id */
from segment
left outer join conjoined on (
    segment.collection_id = conjoined.collection_id and
    segment.key = conjoined.key and
    segment.unified_id = conjoined.unified_id and
    ((segment.handoff_node_id is null and conjoined.handoff_node_id is null)
     or
     segment.handoff_node_id = conjoined.handoff_node_id
    )
);

create or replace view gc_archive as
select 
    archive.*,

    /* add another column with the unified_id a version-specific tombstone
     * points to so that we can create a window with an archive and its
     * tombstone(s) */
    case status = 'T' and file_tombstone_unified_id is not null
        when true then file_tombstone_unified_id
        else unified_id
    end as gc_window_unified_id,

    /* add some additional columns that only have null values
       unless they meet specific conditions.  this lets us use a window
       function across these columns to find, for example, the minimum
       timestamp of a tombstone that applies to a row. */

    case status = 'T' and file_tombstone_unified_id is null
        when true then unified_id
        else null 
    end as gc_general_tombstone_unified_id,

    case status = 'T' and file_tombstone_unified_id is null
        when true then timestamp
        else null 
    end as gc_general_tombstone_timestamp,

    case status = 'T' and file_tombstone_unified_id is not null
        when true then unified_id
        else null 
    end as gc_specific_tombstone_unified_id,

    case status = 'T' and file_tombstone_unified_id is not null
        when true then timestamp
        else null 
    end as gc_specific_tombstone_timestamp,

    /* a single (not conjoined) archive can replace a previous version when it
     * becomes final
     * a conjoined archive can only replace a previosu version when the whole
       conjoined archive becomes complete */
    case 
        when status = 'F' and conjoined_part=0 then unified_id
        when status = 'F' 
             and conjoined_part=1 
             and conjoined_complete_timestamp is not null 
        then unified_id
        else null
    end as gc_archive_unified_id,

    case 
        when status = 'F' and conjoined_part=0 then timestamp
        when status = 'F' 
             and conjoined_part=1 
             and conjoined_complete_timestamp is not null 
        then conjoined_complete_timestamp
        else null
    end as gc_archive_timestamp
from archive;

create or replace view gc_archive_batches as
    select 
        /* from the real segment table */
        gc_archive.*,
        /* window over all the like keys for this collection_id  */
        row_number() over key_rows as key_row_num,
        count(*) over key_rows as key_row_count,
        /* window over all the later segments for this key */
        min(gc_general_tombstone_unified_id) over key_later_rows 
            as gc_next_general_tombstone_unified_id,
        min(gc_general_tombstone_timestamp) over key_later_rows 
            as gc_next_general_tombstone_timestamp,
        min(gc_archive_unified_id) over key_later_rows 
            as gc_next_archive_unified_id,
        min(gc_archive_timestamp) over key_later_rows 
            as gc_next_archive_timestamp,
        /* window over this key and later specific tombstones for this key */
        min(gc_specific_tombstone_unified_id) over key_unified_id_rows
            as gc_next_specific_tombstone_unified_id,
        min(gc_specific_tombstone_timestamp) over key_unified_id_rows
            as gc_next_specific_tombstone_timestamp
    from gc_archive
    /* FIXME need to make sure gc works correctly for handoff rows, and doesn't
     * just ignore them.  */
    where handoff_node_id is null 
    window 
    key_rows as (
        partition by collection_id, key 
        order by unified_id asc, conjoined_part asc
        range between unbounded preceding and unbounded following),
    key_later_rows as (
        partition by collection_id, key 
        order by unified_id asc, conjoined_part asc
        rows between 1 following and unbounded following),
    key_unified_id_rows as (
        partition by collection_id, key, gc_window_unified_id
        order by unified_id asc, conjoined_part asc
        range between unbounded preceding and unbounded following )
    order by collection_id, key, unified_id;

create or replace view gc_archive_batches_with_end_time as
    select 
        gc_archive_batches.*,
        /* several cases, for each of the possibilities */
        least( 
            conjoined_abort_timestamp, conjoined_delete_timestamp,
            /* how soon does a later archive replace this one? */
            case when status='F' and gc_next_archive_unified_id<>unified_id 
            then gc_next_archive_timestamp
            else null end,
            /* how soon does a general tombstone apply? */
            case when status='F' and gc_next_general_tombstone_unified_id<>unified_id
            then gc_next_general_tombstone_timestamp
            else null end,
            /* how soon does a specific tombstone apply */
            case when status='F' and gc_next_specific_tombstone_unified_id<>unified_id
            then gc_next_specific_tombstone_timestamp
            else null end
        ) as unversioned_end_time,
        least(
            /* same as above, except next archive doesn't apply */
            conjoined_abort_timestamp, conjoined_delete_timestamp,

            /* how soon does a general tombstone apply? */
            case when status='F' and gc_next_general_tombstone_unified_id<>unified_id
            then gc_next_general_tombstone_timestamp
            else null end,
            /* how soon does a specific tombstone apply */
            case when status='F' and gc_next_specific_tombstone_unified_id<>unified_id
            then gc_next_specific_tombstone_timestamp
            else null end

        ) as versioned_end_time
    from gc_archive_batches; 

grant select on gc_archive_batches_with_end_time to public;
commit;

/*
explain analyze select * from gc_archive_batches_with_end_time;
explain analyze select * from gc_archive_batches_with_end_time where collection_id=1 and key='a';
select * from gc_archive_batches_with_end_time where versioned_end_time is not null or unversioned_end_time is not null limit 1000;
\x 
explain analyze 
*/

\x
explain analyze
select * from gc_archive where collection_id=1 and key='key-10';
select * from gc_archive_batches_with_end_time where collection_id=1 and key='key-10' order by collection_id, key, unified_id, conjoined_part;

/* 
get the latest archive w/ collection_id, key
get the latest archive w/ collection_id, key, unified_id
list visible archives in collection without limit
list visible archives in collection with limit
list visible archives in collection greater than a marker (for subsequent queries) with limit
*/


CREATE FUNCTION available_archive_unversioned(collection_id int, key text) 
  RETURNS SETOF gc_archive_batches_with_end_time as $$
SELECT *
  FROM gc_archive_batches_with_end_time gc
 WHERE gc.collection_id=$1 and gc.key=$2
   AND gc.status='F'
   AND gc.unversioned_end_time is null
$$ LANGUAGE SQL;

CREATE FUNCTION available_archive_versioned(collection_id int, key text) 
  RETURNS SETOF gc_archive_batches_with_end_time as $$
SELECT *
  FROM gc_archive_batches_with_end_time gc
 WHERE gc.collection_id=$1 and gc.key=$2
   AND gc.status='F'
   AND gc.versioned_end_time is null
$$ LANGUAGE SQL;

CREATE FUNCTION available_archive_unversioned(collection_id int, key text, unified_id int8) 
  RETURNS SETOF gc_archive_batches_with_end_time as $$
SELECT *
  FROM gc_archive_batches_with_end_time gc
 WHERE gc.collection_id=$1 and gc.key=$2 and gc.unified_id=$3
   AND gc.status='F'
   AND gc.unversioned_end_time is null
$$ LANGUAGE SQL;

CREATE FUNCTION available_archive_versioned(collection_id int, key text, unified_id int8)
  RETURNS SETOF gc_archive_batches_with_end_time as $$
SELECT *
  FROM gc_archive_batches_with_end_time gc
 WHERE gc.collection_id=$1 and gc.key=$2 and gc.unified_id=$3
   AND gc.status='F'
   AND gc.versioned_end_time is null
$$ LANGUAGE SQL;

CREATE FUNCTION newest_available_archive_unversioned(collection_id int, key text)
  RETURNS SETOF gc_archive_batches_with_end_time as $$
SELECT * 
  FROM available_archive_unversioned(
       $1, $2, COALESCE((SELECT unified_id 
                           FROM available_archive_unversioned($1, $2) 
                          LIMIT 1), 0))
$$ LANGUAGE SQL;

CREATE FUNCTION newest_available_archive_versioned(collection_id int, key text)
  RETURNS SETOF gc_archive_batches_with_end_time as $$
SELECT * 
  FROM available_archive_versioned(
       $1, $2, COALESCE((SELECT unified_id 
                           FROM available_archive_versioned($1, $2) 
                          LIMIT 1), 0))
$$ LANGUAGE SQL;


CREATE FUNCTION list_available_archive_unversioned(collection_id int)
  RETURNS SETOF gc_archive_batches_with_end_time as $$
SELECT *
  FROM gc_archive_batches_with_end_time gc
 WHERE gc.collection_id=$1
   AND gc.status='F'
   AND gc.unversioned_end_time is null
 ORDER BY key, unified_id
$$ LANGUAGE SQL;

CREATE FUNCTION list_available_archive_versioned(collection_id int)
  RETURNS SETOF gc_archive_batches_with_end_time as $$
SELECT *
  FROM gc_archive_batches_with_end_time gc
 WHERE gc.collection_id=$1
   AND gc.status='F'
   AND gc.versioned_end_time is null
 ORDER BY key, unified_id
$$ LANGUAGE SQL;

CREATE FUNCTION list_available_archive_unversioned(collection_id int, _limit int)
  /* this one is difficult because we need to calculate the limit by returning
   * the right number of unified_ids */
  RETURNS SETOF archive as $$
WITH rows_with_rank as (
SELECT *, 
       dense_rank() over (order by key, unified_id) as dense_rank
  FROM gc_archive_batches_with_end_time gc
 WHERE gc.collection_id=$1
   AND gc.status='F'
   AND gc.unversioned_end_time is null
 ORDER BY key, unified_id
)
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
       conjoined_combined_size,
       conjoined_combined_hash
  FROM rows_with_rank 
 WHERE dense_rank <= $2
 ORDER BY key, unified_id
$$ LANGUAGE SQL;

CREATE FUNCTION list_available_archive_versioned(collection_id int, _limit int)
  RETURNS SETOF archive as $$
WITH rows_with_rank as (
SELECT *, 
       dense_rank() over (order by key, unified_id) as dense_rank
  FROM gc_archive_batches_with_end_time gc
 WHERE gc.collection_id=$1
   AND gc.status='F'
   AND gc.versioned_end_time is null
 ORDER BY key, unified_id
)
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
       conjoined_combined_size,
       conjoined_combined_hash
  FROM rows_with_rank 
 WHERE dense_rank <= $2
 ORDER BY key, unified_id
$$ LANGUAGE SQL;


CREATE FUNCTION list_next_available_archive_unversioned(collection_id int, 
                                                        _limit int,
                                                        key_marker text, 
                                                        unified_id_marker int8)
  RETURNS SETOF archive as $$
WITH rows_with_rank as (
SELECT *, 
       dense_rank() over (order by key, unified_id) as dense_rank
  FROM gc_archive_batches_with_end_time gc
 WHERE gc.collection_id=$1 and gc.key >= $3 and gc.unified_id > $4
   AND gc.status='F'
   AND gc.unversioned_end_time is null
 ORDER BY key, unified_id
)
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
       conjoined_combined_size,
       conjoined_combined_hash
  FROM rows_with_rank 
 WHERE dense_rank <= $2
 ORDER BY key, unified_id
$$ LANGUAGE SQL;

CREATE FUNCTION list_next_available_archive_versioned(collection_id int,
                                                      _limit int, 
                                                      key_marker text, 
                                                      unified_id_marker int8)
  RETURNS SETOF archive as $$
WITH rows_with_rank as (
SELECT *, 
       dense_rank() over (order by key, unified_id) as dense_rank
  FROM gc_archive_batches_with_end_time gc
 WHERE gc.collection_id=$1 and gc.key >= $3 and gc.unified_id > $4
   AND gc.status='F'
   AND gc.versioned_end_time is null
 ORDER BY key, unified_id
)
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
       conjoined_combined_size,
       conjoined_combined_hash
  FROM rows_with_rank 
 WHERE dense_rank <= $2
 ORDER BY key, unified_id
$$ LANGUAGE SQL;


select * from available_archive_unversioned(1, 'key-10');
select * from available_archive_versioned(1, 'key-10');
select * from available_archive_unversioned(1, 'key-10', 150340007);
select * from available_archive_versioned(1, 'key-10', 150340007);
select * from newest_available_archive_unversioned(1, 'key-10');
select * from newest_available_archive_versioned(1, 'key-10');
select * from list_available_archive_unversioned(1);
select * from list_available_archive_versioned(1);
select * from list_available_archive_unversioned(1, 5);
select * from list_available_archive_versioned(1, 5);
select * from list_next_available_archive_unversioned(1, 5, 'key-10', 150340006);
select * from list_next_available_archive_versioned(1, 5, 'key-10', 150340006);

\x
explain
with available_archive_unversioned as (
SELECT *
  FROM gc_archive_batches_with_end_time gc
 WHERE gc.collection_id=1 and gc.key='key-10'
   AND gc.status='F'
   AND gc.unversioned_end_time is null
)
SELECT * 
  FROM available_archive_unversioned 
 WHERE unified_id=COALESCE((SELECT unified_id 
                              FROM available_archive_unversioned
                             LIMIT 1), 0);


CREATE FUNCTION list_available_archive_versioned(collection_id int)
  RETURNS SETOF gc_archive_batches_with_end_time as $$
$$ LANGUAGE SQL;

CREATE FUNCTION list_available_archive_unversioned(collection_id int, limit int)
  RETURNS SETOF gc_archive_batches_with_end_time as $$
$$ LANGUAGE SQL;

CREATE FUNCTION list_available_archive_versioned(collection_id int, limit int)
  RETURNS SETOF gc_archive_batches_with_end_time as $$
$$ LANGUAGE SQL;

CREATE FUNCTION list_next_available_archive_unversioned(collection_id, limit int, 
                                                        key_marker text, 
                                                        unified_id_marker int8)
  RETURNS SETOF gc_archive_batches_with_end_time as $$
$$ LANGUAGE SQL;

CREATE FUNCTION list_next_available_archive_versioned(collection_id, limit int, 
                                                      key_marker text, 
                                                      unified_id_marker int8)
  RETURNS SETOF gc_archive_batches_with_end_time as $$
$$ LANGUAGE SQL;


CREATE FUNCTION newest_available_archive_versioned(collection_id int, key varchar(1024))
available_segments(collection_id, versioned, key);
available_segments(collection_id, versioned, key, unified_id);

newest_available_segments(collection_id, versioned, key);
list_available_segments(collection_id, versioned);
list_available_segments(collection_id, versioned, limit);
list_next_available_segments(collection_id, versioned, limit, 
                             key_marker text, unified_id_marker int8);


/*
select * from gc_archive_batches_with_end_time limit 1000;
*/
/*  */

/*
with numbers as (
select generate_series(1, 10) as n
union all 
select generate_series(1, 10) as n
)
select n, count(n) over (order by n), 
       rank() over (order by n),
       dense_rank() over (order by n)
from numbers;
 n  | count | rank | dense_rank 
----+-------+------+------------
  1 |     2 |    1 |          1
  1 |     2 |    1 |          1
  2 |     4 |    3 |          2
  2 |     4 |    3 |          2
  3 |     6 |    5 |          3
  3 |     6 |    5 |          3
  4 |     8 |    7 |          4
  4 |     8 |    7 |          4
  5 |    10 |    9 |          5
  5 |    10 |    9 |          5
  6 |    12 |   11 |          6
  6 |    12 |   11 |          6
  7 |    14 |   13 |          7
  7 |    14 |   13 |          7
  8 |    16 |   15 |          8
  8 |    16 |   15 |          8
  9 |    18 |   17 |          9
  9 |    18 |   17 |          9
 10 |    20 |   19 |         10
 10 |    20 |   19 |         10
(20 rows)

*/
