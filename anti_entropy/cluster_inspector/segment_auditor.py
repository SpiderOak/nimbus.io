# -*- coding: utf-8 -*-
"""
segment_auditor.py

Inspect segment data for errors
"""
import gzip
import logging
import os
import os.path

from tools.sized_pickle import store_sized_pickle
from tools.data_definitions import create_timestamp, \
        parse_timedelta_str, \
        segment_status_final, \
        segment_status_tombstone

from anti_entropy.anti_entropy_util import anti_entropy_dir, \
        anti_entropy_pre_audit,\
        anti_entropy_missing_replicas,\
        anti_entropy_incomplete_finalization,\
        anti_entropy_damaged_records,\
        anti_entropy_missing_tombstones,\
        anti_entropy_database_inconsistancy,\
        compute_meta_repair_file_path, \
        compute_data_repair_file_path

from anti_entropy.cluster_inspector.work_generator import generate_work

_min_segment_age = os.environ.get("NIMBUSIO_MIN_ANTI_ENTROPY_AGE", "days=1")

def _row_key(row):
    return (row["unified_id"], row["conjoined_part"], )

def _first_row_key(segment_data):
    return _row_key(list(segment_data.values())[0]["segment-row"])

def _row_key_check(segment_data):
    row_key_set = set()
    for entry in segment_data.values():
        if entry["segment-row"] is not None:
            row_key_set.add(_row_key(entry["segment-row"]))
    assert len(row_key_set) == 1, str(row_key_set)

def _data_too_recent(segment_data, newest_allowable_timestamp):
    for entry in segment_data.values():
        if entry["segment-row"] is not None and \
           entry["segment-row"]["timestamp"] > newest_allowable_timestamp:
            return True
    return False

def _missing_replicas(segment_data, newest_allowable_timestamp):
    """
    return True if
    1. the earliest segment.timestamp from any node is at least 
       MIN_ANTI_ENTROPY_AGE old
    2. there are at least 8 distinct values for segment.segment_num, 
       including handoff records.
    3. there is at least one node that does not have a segment record, 
       nor is there a handoff destined for that node.
    """
    if _data_too_recent(segment_data, newest_allowable_timestamp):
        return False

    segment_count = 0
    none_count = 0
    for entry in segment_data.values():
        if entry["segment-row"] is None:
            none_count += 1
        else:
            segment_count += 1

    return (segment_count >= 8) and (none_count > 0)

def _missing_tombstones(segment_data, newest_allowable_timestamp):
    """
    return True if
    1. the earliest segment.timestamp from any node is at least 
       MIN_ANTI_ENTROPY_AGE old
    2. some nodes have the record in an Tombstone status, 
       while others have a different status.
    """
    if _data_too_recent(segment_data, newest_allowable_timestamp):
        return False

    tombstone_count = 0
    for entry in segment_data.values():
        assert entry["segment-row"] is not None
        if entry["segment-row"]["status"] == segment_status_tombstone:
            tombstone_count += 1

    return (tombstone_count > 0) and (tombstone_count < 10)

def _incomplete_finalization(segment_data, newest_allowable_timestamp):
    """
    return True if
    1. the earliest segment.timestamp from any node is at least 
       MIN_ANTI_ENTROPY_AGE old
    2. some nodes have the record in an Active status, 
       while others have a Finalization status.
    """
    if _data_too_recent(segment_data, newest_allowable_timestamp):
        return False

    final_count = 0
    for entry in segment_data.values():
        assert entry["segment-row"] is not None
        if entry["segment-row"]["status"] == segment_status_final:
            final_count += 1

    return (final_count > 0) and (final_count < 10)

def _damaged_records(segment_data):
    """
    return True if:
    1. one or more node has entries in the damaged_segment table.
    """
    for entry in segment_data.values():
        assert entry["segment-row"] is not None
        if entry["is-damaged"]:
            return True

    return False

def _database_inconsistancy(segment_data):
    """
    return True if
    1. for the columns "timestamp", "file_size", "file_adler32", "file_hash", 
    and "source_node_id", not all the database records have a consistent value.
    """
    log = logging.getLogger("_database_inconsistancy")
    test_keys = ["timestamp", 
                 "file_size", 
                 "file_adler32", 
                 "file_hash", 
                 "source_node_id"]

    for key in test_keys:
        result_set = set()
        for entry in segment_data.values():
            assert entry["segment-row"] is not None
            result_set.add(entry["segment-row"][key])
        if len(result_set) != 1:
            log.debug("{0} {1} {2}".format(_row_key(entry), key, result_set))
            return True

    return False

def audit_segments(halt_event, work_dir):
    log = logging.getLogger("audit_segments")

    if not os.path.exists(anti_entropy_dir):
        log.info("creating {0}".format(anti_entropy_dir))
        os.mkdir(anti_entropy_dir)

    meta_repair_file_path = compute_meta_repair_file_path()
    meta_repair_file = \
            gzip.GzipFile(filename=meta_repair_file_path, mode="wb")

    data_repair_file_path = compute_data_repair_file_path()
    data_repair_file = \
            gzip.GzipFile(filename=data_repair_file_path, mode="wb")

    counts = {
        "total"                                 : 0,
        anti_entropy_missing_replicas           : 0,
        anti_entropy_incomplete_finalization    : 0,
        anti_entropy_damaged_records            : 0,
        anti_entropy_missing_tombstones         : 0,
        anti_entropy_database_inconsistancy     : 0,
    }

    current_time = create_timestamp()
    min_segment_age = parse_timedelta_str(_min_segment_age) 
    newest_allowable_timestamp = current_time - min_segment_age
    log.info("newest allowable timestamp = {0}".format(
        newest_allowable_timestamp.isoformat()))

    for audit_data in generate_work(work_dir):
        if halt_event.is_set():
            log.info("halt_event is set: exiting")
            return

        assert audit_data["segment-status"] == anti_entropy_pre_audit
        _row_key_check(audit_data["segment-data"])

        counts["total"] += 1
        
        # missing replicas needs to run first, because the other tests
        # assume there are no missing replicas
        if _missing_replicas(audit_data["segment-data"], 
                             newest_allowable_timestamp):
            log.debug("missing_replicas {0}".format(
                _first_row_key(audit_data["segment-data"])))
            counts[anti_entropy_missing_replicas] += 1
            audit_data["segment-status"] = anti_entropy_missing_replicas
            store_sized_pickle(audit_data, data_repair_file)
            continue
        
        # _missing_tombstones needs to run ahead of _incomplete_finalization
        if _missing_tombstones(audit_data["segment-data"],
                               newest_allowable_timestamp):
            log.debug("missing_tombstones {0}".format(
                _first_row_key(audit_data["segment-data"])))
            counts[anti_entropy_missing_tombstones] += 1
            audit_data["segment-status"] = anti_entropy_missing_tombstones
            store_sized_pickle(audit_data, meta_repair_file)
            continue

        if _incomplete_finalization(audit_data["segment-data"],
                                    newest_allowable_timestamp):
            log.debug("incomplete_finalization {0}".format(
                _first_row_key(audit_data["segment-data"])))
            counts[anti_entropy_incomplete_finalization] += 1
            audit_data["segment-status"] = anti_entropy_incomplete_finalization
            store_sized_pickle(audit_data, data_repair_file)
            continue

        if _damaged_records(audit_data["segment-data"]):
            log.debug("damaged_records {0}".format(
                _first_row_key(audit_data["segment-data"])))
            counts[anti_entropy_damaged_records] += 1
            audit_data["segment-status"] = anti_entropy_damaged_records
            store_sized_pickle(audit_data, data_repair_file)
            continue

        if _database_inconsistancy(audit_data["segment-data"]):
            log.debug("database_inconsistancy {0}".format(
                _first_row_key(audit_data["segment-data"])))
            counts[anti_entropy_database_inconsistancy] += 1
            audit_data["segment-status"] = anti_entropy_database_inconsistancy
            store_sized_pickle(audit_data, data_repair_file)
            continue

    meta_repair_file.close()
    data_repair_file.close()

    keys = ["total",
            anti_entropy_missing_replicas,
            anti_entropy_incomplete_finalization,
            anti_entropy_damaged_records,
            anti_entropy_missing_tombstones,
            anti_entropy_database_inconsistancy]

    for key in keys:
        log.info("{0} {1:,}".format(key, counts[key]))

