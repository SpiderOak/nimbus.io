# -*- coding: utf-8 -*-
"""
file_space.py

Utility routines related to the file_space table 
"""
import os
import os.path

from collections import defaultdict

from tools.data_definitions import file_space_template

class FileSpacesError(Exception):
    pass

def load_file_space_info(connection):
    """
    return a dict of lists of file_space rows keyed by purpose
    """
    rows = connection.fetch_all_rows(
        "select %s from nimbusio_node.file_space" % (
        ",".join(file_space_template._fields), ), [])
    result_dict = defaultdict(list)
    for row in rows:
        row_tuple = file_space_template._make(row)
        result_dict[row_tuple.purpose].append(row_tuple)
    result_dict = dict(result_dict.items())

    return result_dict

def file_space_sanity_check(file_space_info, repository_path):
    """
    assert that symlinks exist for all file_space rows 
    """
    for purpose_list in file_space_info.values():
        for file_space_row in purpose_list:
            symlink_path = os.path.join(repository_path, 
                                        str(file_space_row.space_id))
            assert os.path.islink(symlink_path), symlink_path 
            real_path = os.path.realpath(symlink_path)
            assert real_path == file_space_row.path, (real_path, 
                                                      file_space_row.path, )

def available_space_on_volume(path):
    """
    compute the available space on a volume
    """
    statvfs_result = os.statvfs(path)
    return statvfs_result.f_bsize * statvfs_result.f_bavail

def find_least_volume_space_id(purpose, file_space_info):
    """
    choses the volume with the greatest available free space, 
    by doing a vfsstat at the "path" of the table spaces and comparing.
    """
    max_space_id, _ = find_least_volume_space_id_and_path(purpose, 
                                                          file_space_info)
    return max_space_id

def find_least_volume_space_id_and_path(purpose, file_space_info):
    """
    choses the volume with the greatest available free space, 
    by doing a vfsstat at the "path" of the table spaces and comparing.
    This function returns the path along with the id for regular space checks
    """
    max_avail_space = None
    max_space_id = None
    max_space_path = None

    for file_space_row in file_space_info[purpose]:
        avail_space = available_space_on_volume(file_space_row.path)
        if max_avail_space is None or avail_space > max_avail_space:
            max_avail_space = avail_space
            max_space_id = file_space_row.space_id
            max_space_path = file_space_row.path

    if max_space_id is None:
        raise FileSpacesError("No space for purpose '{0}'".format(purpose))

    # we expect path to be used for a check for minimum available space?
    # see Ticket #5866 Nimbus.io should avoid filling disks to capacity 

    return max_space_id, max_space_path
