# -*- coding: utf-8 -*-
"""
rewrite_value_files.py
"""
from collections import defaultdict
import logging
import os
import os.path

from tools.data_definitions import compute_value_file_path

_repository_path = os.environ["NIMBUSIO_REPOSITORY_PATH"]

def _segment_sequence_size_sum(connection, value_file_id):
    (segment_sequence_size_sum, ) = connection.fetch_one_row("""
        select sum(size) from nimbusio_node.segment_sequence
        where value_file_id = %s""", [value_file_id, ])

    return segment_sequence_size_sum

def _find_ids_of_value_files_to_work_on(options, connection):
    """
    Meet one of these criteria:

    * Only value file IDs that are already defragged 
      (i.e. distinct collection count is 1.)
    * they are too big (i.e. their disk size is larger than the sum of 
      the size of segment sequences they store) such that they are too big 
      by > MIN_GC_SAVINGS_SIZE or too big by MIN_GC_SAVINGS_RATIO
    * they are too small (i.e. their disk size is smaller than 
      MAX_VALUE_SIZE_TO_AGG) AND there will be multiple value files to work on
      for this collection_id within this collection effort 
      (it wouldn't make sense to aggregate a single file -- 
      nothing to aggregate it with)
    """
    log = logging.getLogger("_find_ids_of_value_files_to_work_on")
    value_file_ids = set()

    # convert option from megabytes
    min_savings_size = options.min_savings_size * 1024 ** 2
    max_value_file_size_to_agg = options.max_value_file_size_to_agg * 1024 ** 2

    potential_value_file_ids = list()
    collection_id_value_file_counts = defaultdict(int)

    result = connection.fetch_all_rows("""
        select id, collection_ids from nimbusio_node.value_file 
        where distinct_collection_count = 1;""", [])

    for (value_file_id, [collection_id, ]) in result:
        potential_value_file_ids.append((value_file_id, collection_id, ))
        collection_id_value_file_counts[collection_id] += 1

    for (value_file_id, collection_id, ) in potential_value_file_ids:
        value_file_path = compute_value_file_path(_repository_path,
                                                  value_file_id)
        
        value_file_size = os.path.getsize(value_file_path)

        segment_sequence_size = \
                _segment_sequence_size_sum(connection, value_file_id)

        if segment_sequence_size is None:
            continue

        if segment_sequence_size == value_file_size:
            continue

        if segment_sequence_size > value_file_size:
            log.error("{0} size={1} segment_sequece_size={2}".format(
                value_file_id, value_file_size, segment_sequence_size))
            # TODO: we should deal wiht this somehow
            continue

        savings_size = value_file_size - segment_sequence_size
        savings_ratio = float(savings_size) / float(value_file_size) 

        log.debug("{0} size={1} segment={2} savings={3} ratio={4}".format(
            value_file_id, 
            value_file_size, 
            segment_sequence_size,
            savings_size,
            savings_ratio
        ))

        # they are too big (i.e. their disk size is larger than the sum of 
        # the size of segment sequences they store) such that they are too big 
        # by > MIN_GC_SAVINGS_SIZE or too big by MIN_GC_SAVINGS_RATIO
        if savings_size > min_savings_size \
        or savings_ratio > options.min_savings_ratio:
            value_file_ids.add(value_file_id)
            continue

        # they are too small (i.e. their disk size is smaller than 
        # MAX_VALUE_SIZE_TO_AGG) AND there will be multiple value files to 
        # work on for this collection_id within this collection effort 
        if value_file_size < max_value_file_size_to_agg:
            if collection_id_value_file_counts[collection_id] > 1:
                value_file_ids.add(value_file_id)

        return list(value_file_ids)

