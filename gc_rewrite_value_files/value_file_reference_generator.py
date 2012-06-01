# -*- coding: utf-8 -*-
"""
value_file_reference_generator.py
"""
from collections import namedtuple

_ref_row = namedtuple("RefRow", [
    "segment_id",
    "collection_id", 
    "key",
    "unified_id",
    "sequence_num",
    "value_file_id", 
    "value_file_offset",
    "data_size",
    "data_hash",
    "data_adler32",
    "space_id",
    "value_file_size",
    "value_row_num",
    "value_row_count",
])

_query_template = """
/* SQL for garbage collection */
set search_path to nimbusio_node, public;

begin;
/* just put our parameters into the db here so I only have to specify them once
 * */
drop table if exists gc_param;
create temp table gc_param as
    select 
        %(max_agg_size)s as max_agg_size,
        %(min_save_size)s as min_save_size,
        %(min_save_ratio)s::float as min_save_ratio;

/* first, we identify which value files we wish to work on, based on the above
 * parameters */

/* count and size of references to each value file */
drop table if exists gc_value_file_references;
create temp table gc_value_file_references as
select value_file_id, 
       count(*) as ref_count, 
       sum("size") as ref_size 
from segment_sequence
group by value_file_id order by value_file_id;

/* bring everything together, first round of filtering out value files that
 * don't need our attention */
drop table if exists gc_value_file_stats;
create temp table gc_value_file_stats as 
select 
    collection_ids[1] as collection_id,
    *
from gc_value_file_references vfr 
join value_file vf on (vfr.value_file_id=vf.id)
where 
    vf.close_time is not null and 
    vf.distinct_collection_count=1 and
    vf.collection_ids is not null and
    array_length(vf.collection_ids, 1)=1 and
    /* reduce it down to value files that are potentially collectable */
    ( vf.size < (select max_agg_size from gc_param) or
      vf.size - ref_size > (select min_save_size from gc_param) or
      (vf.size - ref_size) / vf.size::float > 
        (select min_save_ratio from gc_param ) );

/* this gives us a window over all the potentially collectable value files per
 * collection, so we can decisions about small files that we would aggregate */
drop table if exists gc_vf_and_collection_stats;
create temp table gc_vf_and_collection_stats as
select 
    row_number() over collection_rows as collection_row_num,
    count(*) over collection_rows as collection_row_count,
    sum(ref_size) over collection_rows as collection_ref_size,
    sum("size") over collection_rows as collection_file_size,
    *
from gc_value_file_stats
window collection_rows as (partition by collection_id
                           order by collection_id
                           range between unbounded preceding 
                                 and unbounded following)
order by collection_id, value_file_id;

/* filter out small files that would have no other files to be aggregated with
 * */
drop table if exists gc_collectable_value_files;
create temp table gc_collectable_value_files as
    select collection_id, value_file_id from gc_vf_and_collection_stats
    where 
    "size" > (select max_agg_size from gc_param) or collection_row_count > 1;

/* gew all the references to our value files sequentially.
 * we'll read them in batches, sort in memory, write sequentially, unlink
 */
select s.id, s.collection_id, s.key, s.unified_id, 
ss.sequence_num, ss.value_file_id, ss.value_file_offset, ss.size, 
ss.hash, ss.adler32, 
vf.space_id, vf.size,
row_number() over value_rows as value_row_num,
count(*) over value_rows as value_row_count
from segment s 
join segment_sequence ss on (s.id=ss.segment_id)
join value_file vf on (ss.value_file_id=vf.id)
where 
    vf.close_time is not null and
    vf.distinct_collection_count=1 and
    vf.collection_ids is not null and 
    array_length(vf.collection_ids, 1)=1 and
    value_file_id in ( select value_file_id from gc_collectable_value_files )
window value_rows as (partition by ss.value_file_id
                      order by ss.value_file_offset
                      range between unbounded preceding and unbounded following)
order by s.collection_id, ss.value_file_id, ss.value_file_offset;
"""

def generate_value_file_references(options, connection):
    """
    get references from the database
    """
    query_dict = {
        "max_agg_size"      : options.max_value_file_size_to_agg * 1024 ** 2,
        "min_save_size"     : options.min_savings_size * 1024 ** 2,
        "min_save_ratio"    : options.min_savings_ratio,
    }
    for result in connection.generate_all_rows(_query_template, query_dict):
        yield _ref_row._make(result)

