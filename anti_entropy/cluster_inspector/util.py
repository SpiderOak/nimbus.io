# -*- coding: utf-8 -*-
"""
util.py

utility routines shared wiht subprocesses
"""
import os.path

def compute_segment_file_path(work_dir, node_name):
    segment_file_name = "segment-{0}.gzip".format(node_name)
    return os.path.join(work_dir, segment_file_name)

def compute_damaged_segment_file_path(work_dir, node_name):
    segment_file_name = "damaged-segment-{0}.gzip".format(node_name)
    return os.path.join(work_dir, segment_file_name)

