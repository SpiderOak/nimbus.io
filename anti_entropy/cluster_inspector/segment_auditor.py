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

def _missing_replicas(segment_data):
    return False

def _incomplete_finalization(segment_data):
    return False

def _damaged_records(segment_data):
    return False

def _missing_tombstones(segment_data):
    return False

def _database_inconsistancy(segment_data):
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


    for audit_data in generate_work(work_dir):
        if halt_event.is_set():
            log.info("halt_event is set: exiting")
            return

        assert audit_data["segment-status"] == anti_entropy_pre_audit

        counts["total"] += 1

        if _missing_replicas(audit_data["segment-data"]):
            counts[anti_entropy_missing_replicas] += 1
            audit_data["segment-status"] = anti_entropy_missing_replicas
            store_sized_pickle(audit_data, data_repair_file)
            continue

        if _incomplete_finalization(audit_data["segment-data"]):
            counts[anti_entropy_incomplete_finalization] += 1
            audit_data["segment-status"] = anti_entropy_incomplete_finalization
            store_sized_pickle(audit_data, data_repair_file)
            continue

        if _damaged_records(audit_data["segment-data"]):
            counts[anti_entropy_damaged_records] += 1
            audit_data["segment-status"] = anti_entropy_damaged_records
            store_sized_pickle(audit_data, data_repair_file)
            continue

        if _missing_tombstones(audit_data["segment-data"]):
            counts[anti_entropy_missing_tombstones] += 1
            audit_data["segment-status"] = anti_entropy_missing_tombstones
            store_sized_pickle(audit_data, meta_repair_file)
            continue

        if _database_inconsistancy(audit_data["segment-data"]):
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

