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
    # not needed because we are in auto-commit mode.
    # connection.rollback()
    assert not connection._in_transaction

    # 3) do the "select id from value_file" and store in a set
    database_value_file_ids = dict()
    for (value_file_id, space_id, ) in connection.fetch_all_rows(
        "select id, space_id from nimbusio_node.value_file", []
    ):  
        database_value_file_ids[value_file_id] = space_id

    # 4) unlink things from #1 not present in #3.
    unreachable_value_file_ids = filesystem_value_file_ids \
                               - set(database_value_file_ids)

    total_value_file_size = 0
    for value_file_id in list(unreachable_value_file_ids):
        value_file_path = compute_value_file_path(repository_path,
            database_value_file_ids[value_file_id], value_file_id)
        value_file_size = os.path.getsize(value_file_path)
        log.info("unlinking unreachable value_file {0} size = {1}".format(
            value_file_path, value_file_size
        ))
        total_value_file_size += value_file_size
        try:
            os.unlink(value_file_path)
        except Exception:
            log.exception(value_file_path)

    log.info(
        "found {0:,} unreachable value files; savings={1:,}".format(
            len(unreachable_value_file_ids), total_value_file_size
        )
    )

    return total_value_file_size

