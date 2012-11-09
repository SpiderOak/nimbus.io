# -*- coding: utf-8 -*-
"""
process_segment_rows.py

process handoffs of segment rows
"""
import logging
import itertools

def _key_function(segment_row):
    return (segment_row["unified_id"], segment_row["conjoined_part"], )

def process_segment_rows(segment_rows):
    """
    process handoffs of segment rows
    """
    log = logging.getLogger("process_segment_row")
    # sort on (unified_id, conjoined_part) to bring pairs together
    segment_rows.sort(key=_key_function)
    group_object = itertools.groupby(segment_rows, _key_function)

    for (unified_id, conjoined_id, ), group in group_object:
        log.info("{0} {1} {2}".format((unified_id, conjoined_id),
                                      type(group),
                                      list(group)))
