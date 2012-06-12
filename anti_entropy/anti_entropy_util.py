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
anti_entropy_damaged_records = "damaged-records"
anti_entropy_missing_tombstones = "missing-tombstones"
anti_entropy_database_inconsistancy = "database-inconsistency"

def compute_meta_repair_file_path():
    file_name = "meta_repair.gzip"
    return os.path.join(anti_entropy_dir, file_name)

def compute_data_repair_file_path():
    file_name = "data_repair.gzip"
    return os.path.join(anti_entropy_dir, file_name)

