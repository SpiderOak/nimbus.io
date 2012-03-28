create or replace temporary view sequence_value as
select sq.segment_id, sq.sequence_num, sq.value_file_offset, 
sq.size as sequence_size,
vf.close_time, vf.size as value_file_size, vf.hash as value_file_hash
from nimbusio_node.segment_sequence sq left join nimbusio_node.value_file vf 
on sq.value_file_id = vf.id;

select seg.collection_id, seg.key, seg.unified_id, seg.conjoined_part, 
seg.segment_num, sv.sequence_num, sv.value_file_offset, 
sv.sequence_size, sv.close_time, sv.value_file_size, sv.value_file_hash
from nimbusio_node.segment seg left join sequence_value sv
on sv.segment_id = seg.id
where seg.status = 'F'
order by seg.collection_id, seg.key, seg.unified_id, seg.conjoined_part 
