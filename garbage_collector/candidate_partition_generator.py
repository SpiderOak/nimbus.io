# -*- coding: utf-8 -*-
"""
candidate partition generator
"""
from collections import namedtuple

_partition_entry = namedtuple("PartitionEntry", [
    "segment_id", 
    "collection_id", 
    "key", 
    "status",
    "unified_id", 
    "file_tombstone_unified_id",
    "key_row_number",
    "key_row_count",])

_multiple_rows_for_key_query = """
set search_path to nimbusio_node, public;
with batched_rows as (
    select id, collection_id, key, status, unified_id, 
        file_tombstone_unified_id,
        row_number() over key_rows as key_row_num,
        count(*) over key_rows as key_row_count
        from segment
        where handoff_node_id is null 
    window key_rows as (partition by collection_id, key order by unified_id asc
            range between unbounded preceding and unbounded following 
    )
    order by collection_id, key, unified_id
)
select * from batched_rows where key_row_count > 1;
"""

def _test_partition(partition):
    """
    Consistency checks suggested by Alan 
    """
    assert [r.key_row_number for r in partition] == \
        list(range(1, 1 + len(partition)))
    assert all([r.key_row_count == len(partition) for r in partition])

def generate_candidate_partitions(connection):
    """
    * Select all records ordered by collection_id, key, unified_id, 
      having more than one row per collection_id and key
    * Gather rows into partitions within memory. 
      A partition is all rows for the same collection_id and key 
      (the same thing that the SQL window functions are partitioning by. 
      So at the start of every partition, key_row_num=1, 
      and at the end of every partition, key_row_num=key_row_count.)
    * Yield one partition at a time
    """
    current_partition_id = None
    current_partition = list()
    for row in connection.generate_all_rows(_multiple_rows_for_key_query, []):
        entry = _partition_entry._make(row)
        partition_id = (entry.collection_id, entry.key, )

        if current_partition_id is None:
            current_partition_id = partition_id
        elif partition_id != current_partition_id:
            _test_partition(current_partition)
            yield current_partition
            current_partition = list()
            current_partition_id = partition_id
        
        current_partition.append(entry)

    if current_partition_id is not None:
        _test_partition(current_partition)
        yield current_partition

