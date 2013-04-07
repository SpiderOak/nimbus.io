CREATE temp table _tmp_new_garbage AS 
SELECT 
       segment_id as segment_id,
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
       conjoined_combined_hash,
       current_timestamp as collected_time,
       least(collection_deletion_time, 
             conjoined_abort_timestamp, 
             conjoined_delete_timestamp,
             unversioned_end_time,
             versioned_end_time) as collected_end_time,
       null::int8 as collected_by_unified_id
FROM (
SELECT
       segment_id,
       gcabwet.collection_id,
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
       conjoined_combined_hash,
       current_timestamp as collected_time,
       case when collection_is_versioned=true 
            then versioned_end_time 
            else null 
            end as versioned_end_time,
       case when collection_is_versioned is null
            then unversioned_end_time
            else null
            end as unversioned_end_time,
       collection_deletion_time
  FROM nimbusio_node.gc_archive_batches_with_end_time gcabwet
  LEFT JOIN _tmp_versioned_collections 
    ON (gcabwet.collection_id = _tmp_versioned_collections.collection_id)
  LEFT JOIN _tmp_collection_del_times
    ON (gcabwet.collection_id = _tmp_collection_del_times.collection_id)
 WHERE handoff_node_id IS NULL AND (
        ( status = 'C' or 
          conjoined_abort_timestamp is not null or
          conjoined_delete_timestamp is not null
        )
        OR
        ( status = 'T' and age(timestamp) > '1 days'::interval )
        OR
        (status = 'F' and 
         (
          (collection_deletion_time is not null) or
          (collection_is_versioned = true and 
           versioned_end_time is not null) or
          (collection_is_versioned is null and 
           unversioned_end_time is not null)
         )
        )
       )
) a
