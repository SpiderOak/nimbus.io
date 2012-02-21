"""
unreachable_value_files.py

clean out value files that are unreachable from the database
"""
import logging
import os
import os.path

from tools.data_definitions import compute_value_file_path

def unlink_unreachable_value_files(connection, repository_path):
    """
    clean out value files that are unreachable from the database
    """
    log = logging.getLogger("unlink_unreachable_value_files")

    # 1) build up an accumulator of value files seen in the file system.
    # the repository contains some instances of directories 000-999
    # where 'nnn' is value_file.id mod 1000
    # in those directories, we're looking for filenames that are 
    # 8 digit numbers, representing value_file.id
    filesystem_value_file_ids = set()
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
                filesystem_value_file_ids.add(value_file_id)

    # 2) explicitly rollback the connection's current transaction
    connection.rollback()

    # 3) do the "select id from value_file" and store in a set
    database_value_file_ids = set()
    for (value_file_id, ) in connection.fetch_all_rows(
        "select id from nimbusio_node.value_file", []
    ):  
        database_value_file_ids.add(value_file_id)

    # 4) unlink things from #1 not present in #3.
    unreachable_value_file_ids = filesystem_value_file_ids \
                               - database_value_file_ids

    for value_file_id in list(unreachable_value_file_ids):
        value_file_path = compute_value_file_path(repository_path, 
                                                  value_file_id)
        log.info("unlinking unreachable value_file {0}".format(
            value_file_path
        ))
        try:
            os.unlink(value_file_path)
        except Exception:
            log.exception(value_file_path)

