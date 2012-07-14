# -*- coding: utf-8 -*-
"""
work_generator.py
"""
from collections import namedtuple

_entry_template = namedtuple("WorkEntry", [
    "collection_id", 
    "key", 
    "unified_id", 
    "conjoined_part", 
    "timestamp",
    "segment_num", 
    "file_size",
    "sequence_num", 
    "value_file_offset", 
    "sequence_size", 
    "zfec_padding_size",
    "sequence_hash",
    "value_file_id", 
    "value_file_close_time", 
    "value_file_size", 
    "value_file_hash",
    "value_file_last_integrity_check_time",
    "space_id",
])

_work_query = """
select seg.collection_id, seg.key, seg.unified_id, seg.conjoined_part, 
seg.timestamp, seg.segment_num, seg.file_size, 
sq.sequence_num, sq.value_file_offset, sq.size as sequence_size, 
sq.zfec_padding_size, sq.hash,
vf.id as value_file_id, vf.close_time, vf.size as value_file_size, 
vf.hash as value_file_hash, vf.last_integrity_check_time, vf.space_id
from nimbusio_node.segment seg 
left join nimbusio_node.segment_sequence sq  on (sq.segment_id = seg.id)
left join nimbusio_node.value_file vf on (sq.value_file_id = vf.id)
where 
space_id not in (select space_id 
                 from nimbusio_node.file_space 
                 where purpose='journal')
and seg.status = 'F'
order by seg.collection_id, seg.key, seg.unified_id, seg.conjoined_part,
sq.sequence_num
"""
def make_batch_key(entry):
    return (entry.unified_id, entry.conjoined_part, entry.segment_num, )

def generate_work(connection):
    """
    generate batches for inspection
    """
    prev_key = None
    batch = list()
    for raw_entry in connection.generate_all_rows(_work_query, []):
        entry = _entry_template._make(raw_entry)
        batch_key = make_batch_key(entry)
        if prev_key is None:
            prev_key = batch_key
        if batch_key != prev_key:
            yield batch
            batch = list()
            prev_key = batch_key
        batch.append(entry)

    if len(batch) is not None:
        yield batch

if __name__ == "__main__":
    """
    test the generator independantly
    """
    from tools.database_connection import get_node_local_connection
    connection = get_node_local_connection()
    for entry in generate_work(connection):
        print(entry)
