/* generate abundant test data for stressing/testing the segment visibility
 * subsystem.

   Generally, try to generate a large number of archives that cover every
   possibility of configuration that can cause an archive to be
   visible/garbage, etc.  Include conjoined parts, canceled archives, multiple
   versions of many keys, and general and specific tombstones at all points
   along the version timeline.
 */

begin;
set search_path to nimbusio_node, public;

/* add a median function, from the postgresql wiki */
CREATE OR REPLACE FUNCTION _final_median(anyarray)
   RETURNS float8 AS
$$ 
  WITH q AS
  (
     SELECT val
     FROM unnest($1) val
     WHERE VAL IS NOT NULL
     ORDER BY 1
  ),
  cnt AS
  (
    SELECT COUNT(*) AS c FROM q
  )
  SELECT AVG(val)::float8
  FROM 
  (
    SELECT val FROM q
    LIMIT  2 - MOD((SELECT c FROM cnt), 2)
    OFFSET GREATEST(CEIL((SELECT c FROM cnt) / 2.0) - 1,0)  
  ) q2;
$$
LANGUAGE sql IMMUTABLE;
 
DROP AGGREGATE IF EXISTS median(anyelement);
CREATE AGGREGATE median(anyelement) (
  SFUNC=array_append,
  STYPE=anyarray,
  FINALFUNC=_final_median,
  INITCOND='{}'
);


truncate segment;
explain analyze
insert into segment (
    id, collection_id, key, status, unified_id, timestamp, segment_num, 
    conjoined_part, file_size, file_adler32, file_hash, source_node_id )
select 
    10 * id.id as id, /* make the ID's 10 apart so we can have room to
                         insert conjoined parts, tombstones, etc. */
    1 + (100 * random())::int4 as collection_id,
    'key-' || (1 + (100 * random()))::int4::text as key,
    'F' as status,
    current_timestamp::abstime::int4::int8
        - '2011-01-01'::timestamp::abstime::int4::int8
        + (1000::int8 * id::int8) as unified_id,
    current_timestamp - ('10000000 seconds'::interval)
                      + ('1 second'::interval * id) as timestamp,
    1 as segment_num,
    0 as conjoined_part,
    (1000000000 * random())::int8 as file_size,
    0 as file_adler32,
    'ffffffffffffffff' as file_hash,
    1 as source_node_id
from 
generate_series(1, 100000) id;

/* make some of the archives be conjoined archives */
update segment set conjoined_part=1 where random() < 0.05;
/* every part 1 needs a part 2 */
insert into segment (
        id, collection_id, key, status, unified_id, timestamp, segment_num, 
        conjoined_part, file_size, file_adler32, file_hash, source_node_id )
    select 
        id+1, collection_id, key, status, unified_id, timestamp + '1 second'::interval, segment_num, 
        conjoined_part + 1, file_size, file_adler32, file_hash, source_node_id
    from segment
    where conjoined_part=1;
/* create a part 3 for half of the part 2s */
insert into segment (
        id, collection_id, key, status, unified_id, timestamp, segment_num, 
        conjoined_part, file_size, file_adler32, file_hash, source_node_id )
    select 
        id+1, collection_id, key, status, unified_id, timestamp + '1 second'::interval, segment_num, 
        conjoined_part + 1, file_size, file_adler32, file_hash, source_node_id
    from segment
    where conjoined_part=2 and random() > 0.5;
/* create a part 4 for half of the part 3s */
insert into segment (
        id, collection_id, key, status, unified_id, timestamp, segment_num, 
        conjoined_part, file_size, file_adler32, file_hash, source_node_id )
    select 
        id+1, collection_id, key, status, unified_id, timestamp + '1 second'::interval, segment_num, 
        conjoined_part + 1, file_size, file_adler32, file_hash, source_node_id
    from segment
    where conjoined_part=3 and random() > 0.5;

/* add the records to the conjoined table */
insert into conjoined (id, collection_id, key, unified_id, create_timestamp, complete_timestamp ) 
    select row_number() over (), collection_id, key, unified_id, 
    "timestamp" - '5 seconds'::interval as create_timestamp,
    "timestamp" + '10 seconds'::interval as complete_timestamp
    from segment 
    where conjoined_part=1 
    order by id;
/* make some conjoined entries aborted or deleted */
update conjoined set abort_timestamp=complete_timestamp, 
                     complete_timestamp=null 
    where random() > 0.9;
update conjoined set delete_timestamp=complete_timestamp + '1 day'::interval 
    where abort_timestamp is null 
      and random() > 0.9;

select setval('conjoined_id_seq', 1 + (select max(id) from conjoined));
select setval('segment_id_seq', 1 + (select max(id) from segment));

/* add tombstones */
create temp table key_unified_id_range as
    select 
        collection_id, key, min(unified_id) as min, 
                            max(unified_id) as max, 
                            median(unified_id)::int8 as median,
                            count(*) as count
    from segment
    where conjoined_part in ( 0, 1 )
    group by collection_id, key
    order by collection_id, key;

create temp table existing_unified_ids as
    select distinct unified_id as unified_id from segment;
create index existing_unified_ids_idx on existing_unified_ids (unified_id);

drop index if exists segment_unified_id_idx;
create index segment_unified_id_idx on segment (unified_id);

/* insert general tombstone before earliest unified_id for key */
insert into segment (
        id, 
        collection_id, key, status, 
        unified_id, 
        timestamp, 
        segment_num, conjoined_part, file_size, file_adler32, file_hash, 
        source_node_id )
    select 
        nextval('nimbusio_node.segment_id_seq'), 
        collection_id, key, 'T', 
        min - 1 as unified_id, 
        (select min(timestamp) from segment s2 where s2.unified_id=min),
        1, 0, 0, null, null, 1
    from key_unified_id_range
    where random() < 0.05
      and not exists (select 1 from segment s2 where s2.unified_id=min - 1);


/* insert general tombstone after earliest unified_id for key */
insert into segment (
        id, 
        collection_id, key, status, 
        unified_id, 
        timestamp, 
        segment_num, conjoined_part, file_size, file_adler32, file_hash, 
        source_node_id )
    select 
        nextval('nimbusio_node.segment_id_seq'), 
        collection_id, key, 'T', 
        min + 1 as unified_id, 
        (select min(timestamp)+'1 second'::interval 
         from segment s2 where s2.unified_id=min),
        1, 0, 0, null, null, 1
    from key_unified_id_range
    where random() < 0.05
      and not exists (select 1 from segment s2 where s2.unified_id=min + 1);

/* insert general tombstone before median unified_id for key */
insert into segment (
        id, 
        collection_id, key, status, 
        unified_id, 
        timestamp, 
        segment_num, conjoined_part, file_size, file_adler32, file_hash, 
        source_node_id )
    select 
        nextval('nimbusio_node.segment_id_seq'), 
        collection_id, key, 'T', 
        median - 1 as unified_id, 
        (select timestamp-'1 second'::interval 
         from segment s2 where s2.unified_id>=median 
         order by unified_id limit 1),
        1, 0, 0, null, null, 1
    from key_unified_id_range
    where random() < 0.05
      and not exists (select 1 from segment s2 where s2.unified_id=median - 1);

/* insert general tombstone after median unified_id for key */
insert into segment (
        id, 
        collection_id, key, status, 
        unified_id, 
        timestamp, 
        segment_num, conjoined_part, file_size, file_adler32, file_hash, 
        source_node_id )
    select 
        nextval('nimbusio_node.segment_id_seq'), 
        collection_id, key, 'T', 
        median + 1 as unified_id, 
        (select timestamp-'1 second'::interval 
         from segment s2 where s2.unified_id>=median 
         order by unified_id limit 1),
        1, 0, 0, null, null, 1
    from key_unified_id_range
    where random() < 0.05
      and not exists (select 1 from segment s2 where s2.unified_id=median + 1);

/* insert general tombstone before last unified_id for key */
insert into segment (
        id, 
        collection_id, key, status, 
        unified_id, 
        timestamp, 
        segment_num, conjoined_part, file_size, file_adler32, file_hash, 
        source_node_id )
    select 
        nextval('nimbusio_node.segment_id_seq'), 
        collection_id, key, 'T', 
        max - 1 as unified_id, 
        (select min(timestamp)-'1 second'::interval 
         from segment s2 where s2.unified_id=max),
        1, 0, 0, null, null, 1
    from key_unified_id_range
    where random() < 0.05
      and not exists (select 1 from segment s2 where s2.unified_id=max - 1);

/* insert general tombstone after last unified_id for key */
insert into segment (
        id, 
        collection_id, key, status, 
        unified_id, 
        timestamp, 
        segment_num, conjoined_part, file_size, file_adler32, file_hash, 
        source_node_id )
    select 
        nextval('nimbusio_node.segment_id_seq'), 
        collection_id, key, 'T', 
        max + 1 as unified_id, 
        (select min(timestamp)+'1 second'::interval 
         from segment s2 where s2.unified_id=max),
        1, 0, 0, null, null, 1
    from key_unified_id_range
    where random() < 0.05
      and not exists (select 1 from segment s2 where s2.unified_id=max + 1);


/* create some version specific tombstones */
insert into segment (
        id, 
        collection_id, key, status, 
        unified_id, 
        segment_num, conjoined_part, file_size, file_adler32, file_hash, 
        source_node_id,
        file_tombstone_unified_id, timestamp )
    with archives_to_get_tombstones as (
        select 
            nextval('nimbusio_node.segment_id_seq'), 
            collection_id, key, 'T', 
            unified_id::int8 + (10000 * random())::int8 as unified_id,
            1 as segment_num, 0 as conjoined_part, 0 as file_size, 
              0 as file_adler32, null::bytea as file_hash, 1 as source_node_id,
            unified_id::int8 as file_tombstone_unified_id
        from segment
        where random() < 0.05
          and status='F'
          and conjoined_part in ( 0, 1 )
    )
    select 
        a.*,
        coalesce((select timestamp+'1 second'::interval 
         from segment s2 where s2.unified_id > a.unified_id 
         order by unified_id limit 1), current_timestamp) as timestamp
    from archives_to_get_tombstones a
    where not exists (
        select 1 from segment s2 where s2.unified_id=a.unified_id);

commit;

/*
    ) a;
with segment_action_driver as (
    select segment.*, 
        (100 * random())::int4 as driver
    from segment
    limit 10
)
select 
    *,
    case 
        when driver between 1 and 10 
        then 'do1'
        when driver between 11 and 20 
        then 'do2'
        else 'do nothing'
    end as action
from segment_action_driver;
*/
