# -*- coding: utf-8 -*-
"""
anti_entropy_util.py

utility code shared by anti-entropy programs
"""
import os
import os.path

_repository_path = os.environ["NIMBUSIO_REPOSITORY_PATH"]      
anti_entropy_dir = os.path.join(_repository_path, "cluster_inspector")

anti_entropy_pre_audit = "pre-audit"
anti_entropy_missing_replicas = "missing-replicas"
anti_entropy_incomplete_finalization = "incomplete-finalization"
anti_entropy_damaged_records ="damaged-records"
anti_entropy_missing_tombstones = "missing-tombstones"
anti_entropy_database_inconsistancy = "database-inconsistency"

def compute_meta_repair_file_path():
    file_name = "meta_repair.gzip"
    return os.path.join(anti_entropy_dir, file_name)

def compute_data_repair_file_path():
    file_name = "data_repair.gzip"
    return os.path.join(anti_entropy_dir, file_name)

def identify_program_dir(target_dir):
    python_path = os.environ["PYTHONPATH"]
    for work_path in python_path.split(os.pathsep):
        test_path = os.path.join(work_path, target_dir)
        if os.path.isdir(test_path):
            return test_path

    raise ValueError(
        "Can't find %s in PYTHONPATH '%s'" % (target_dir, python_path, )
    )

