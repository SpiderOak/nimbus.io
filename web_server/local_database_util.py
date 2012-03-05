# -*- coding: utf-8 -*-
"""
local_database_util.py

utility routines for the node local database
"""
from collections import namedtuple
import logging

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
    order by seg.unified_id desc, seg.conjoined_part asc;
"""

_version_query = """
    select con.create_timestamp, con.complete_timestamp, 
    seg.id, seg.status, seg.unified_id, seg.conjoined_part, seg.timestamp, 
    seg.file_size, seg.file_hash
    from nimbusio_node.conjoined con right outer join nimbusio_node.segment seg 
    on con.unified_id = seg.unified_id 
    where seg.unified_id = %s 
    order by seg.conjoined_part asc;
"""

def current_status_of_key(connection, collection_id, key):
    """
    retrieve the conjoined row (if any) and most current related
    segment_rows for this key

    return a list of status rows
    """
    log = logging.getLogger("current_status_of_key")
    status_rows = list()
    newest_unified_id = None
    result = connection.fetch_all_rows(_key_query, [collection_id, key, ])
    for raw_row in result:
        row = _status_row_template._make(raw_row)
        if newest_unified_id is None:
            newest_unified_id = row.seg_unified_id
        if row.seg_unified_id != newest_unified_id:
            break
        status_rows.append(row)
    
    log.debug(str(status_rows))
    return status_rows

def current_status_of_version(connection, version_id):
    """
    retrieve the conjoined row (if any) and most current related
    segment_rows for this version_id

    return a list of status rows
    """
    log = logging.getLogger("current_status_of_version")
    result = connection.fetch_all_rows(_version_query, [version_id, ])
    log.info(str(list(result)))
    status_rows = [_status_row_template._make(raw_row) for raw_row in result]

    log.debug(str(status_rows))
    return status_rows

