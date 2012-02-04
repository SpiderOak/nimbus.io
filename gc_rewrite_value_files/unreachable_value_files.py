# -*- coding: utf-8 -*-
"""
unreachable_value_files.py

clean out value files that are unreachable from the database
"""
import logging
import os
import os.path

_reachable_query = """
select count(*) from nimbusio_node.value_file where id = %s
"""

def _value_file_is_reachable(connection, value_file_id):
    (count, ) = connection.fetch_one_row(_reachable_query, [value_file_id, ])
    return count == 1

def unlink_unreachable_value_files(connection, repository_path):
    """
    clean out value files that are unreachable from the database
    """
    log = logging.getLogger("unlink_unreachable_value_files")

    # the repository contains some instances of directories 000-999
    # where 'nnn' is value_file.id mod 1000
    # in those directories, we're looking for filenames that are 
    # 8 digit numbers, representing value_file.id
    for i in range(1000):
        dir_name = "{0:0>3}".format(i)
        dir_path = os.path.join(repository_path, dir_name)
        if os.path.isdir(dir_path):
            for name in os.listdir(dir_path):
                if len(name) != 8:
                    continue
                try:
                    value_file_id = int(name)
                except ValueError:
                    continue
                if not _value_file_is_reachable(connection, value_file_id):
                    full_path = os.path.join(dir_path, name)
                    log.info("unlinking unreachable value_file {0}".format(
                        full_path
                    ))
                    try:
                        os.unlink(full_path)
                    except Exception:
                        log.exception(full_path)

