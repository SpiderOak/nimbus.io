# -*- coding: utf-8 -*-
"""
local_database_util.py

utility routines for the node local database
"""
import os
from collections import namedtuple
import logging

_local_node_name = os.environ["NIMBUSIO_NODE_NAME"]
_status_row_template = namedtuple("StatusRow", [
    "con_create_timestamp",
    "con_complete_timestamp",
    "seg_id",
    "seg_status",
    "seg_unified_id",
    "seg_conjoined_part",
    "seg_timestamp",
    "seg_file_size",
    "seg_file_hash",
])

_key_query = """
    select con.create_timestamp, con.complete_timestamp, 
    seg.id, seg.status, seg.unified_id, seg.conjoined_part, seg.timestamp, 
    seg.file_size, seg.file_hash
    from nimbusio_node.conjoined con right outer join nimbusio_node.segment seg 
    on con.unified_id = seg.unified_id 
    where seg.collection_id = %s and seg.key=%s
    and seg.handoff_node_id is null
    and con.handoff_node_id is null
    order by seg.unified_id desc, seg.conjoined_part asc;
"""

_version_query = """
    select con.create_timestamp, con.complete_timestamp, 
    seg.id, seg.status, seg.unified_id, seg.conjoined_part, seg.timestamp, 
    seg.file_size, seg.file_hash
    from nimbusio_node.conjoined con right outer join nimbusio_node.segment seg 
    on con.unified_id = seg.unified_id 
    where seg.key = %s and seg.unified_id = %s 
    and seg.handoff_node_id is null
    and con.handoff_node_id is null
    order by seg.conjoined_part asc;
"""

def _construct_row(result_dict):
    return _status_row_template(
        con_create_timestamp=result_dict["create_timestamp"],
        con_complete_timestamp=result_dict["complete_timestamp"],
        seg_id=result_dict["id"],
        seg_status=result_dict["status"],
        seg_unified_id=result_dict["unified_id"],
        seg_conjoined_part=result_dict["conjoined_part"],
        seg_timestamp=result_dict["timestamp"],
        seg_file_size=result_dict["file_size"],
        seg_file_hash=result_dict["file_hash"])

def current_status_of_key(interaction_pool, collection_id, key):
    """
    retrieve the conjoined row (if any) and most current related
    segment_rows for this key

    return a list of status rows
    """
    log = logging.getLogger("current_status_of_key")
    status_rows = list()
    newest_unified_id = None
    async_result = interaction_pool.run(interaction=_key_query, 
                                        interaction_args=[collection_id, 
                                                          key, ],
                                        pool=_local_node_name)
    result = async_result.get()
    for raw_row in result:
        row = _construct_row(raw_row)
        if newest_unified_id is None:
            newest_unified_id = row.seg_unified_id
        if row.seg_unified_id != newest_unified_id:
            break
        status_rows.append(row)
    
    return status_rows

def current_status_of_version(interaction_pool, version_id, key):
    """
    retrieve the conjoined row (if any) and most current related
    segment_rows for this version_id

    return a list of status rows
    """
    log = logging.getLogger("current_status_of_version")
    async_result = interaction_pool.run(interaction=_version_query, 
                                        interaction_args=[key, version_id, ],
                                        pool=_local_node_name)
    result = async_result.get()
    status_rows = [_construct_row(raw_row) for raw_row in result]

    return status_rows

