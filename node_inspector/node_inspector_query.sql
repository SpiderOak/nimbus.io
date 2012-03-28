select seg.collection_id, seg.key, seg.unified_id, seg.conjoined_part, 
seg.segment_num, 
sq.sequence_num, sq.value_file_offset, sq.size as sequence_size, 
vf.id as value_file_id, vf.close_time, vf.size as value_file_size, 
vf.hash as value_file_hash
from nimbusio_node.segment seg 
left join nimbusio_node.segment_sequence sq  on (sq.segment_id = seg.id)
left join nimbusio_node.value_file vf on (sq.value_file_id = vf.id)
where seg.status = 'F'
order by seg.collection_id, seg.key, seg.unified_id, seg.conjoined_part 
