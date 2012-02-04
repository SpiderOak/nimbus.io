# -*- coding: utf-8 -*-
"""
unused value files
unlink totally unused value files
"""
import logging
import os

from tools.data_definitions import value_file_template, compute_value_file_path

_unused_value_files_query = """
with used_value_files as (
    select distinct value_file_id as id
    from nimbusio_node.segment_sequence
)
select id from nimbusio_node.value_file where 
    close_time is not null and
    not exists (
        select 1 from used_value_files uvf 
        where uvf.id=value_file.id);
"""

_delete_value_file_query = """
    delete from nimbusio_node.value_file where id = %s
"""

def _list_unused_value_file_ids(connection):
    result = connection.fetch_all_rows(_unused_value_files_query, [])
    return [value_file_id for (value_file_id, ) in result]

def _unlink_value_files(connection, repository_path, unused_value_file_ids):
    log = logging.getLogger("_unlink_value_files")
    for value_file_id in unused_value_file_ids:
        connection.execute(_delete_value_file_query, [value_file_id, ])
        value_file_path = compute_value_file_path(
            repository_path, value_file_id
        )
        try:
            os.unlink(value_file_path)
        except Exception:
            log.exception(value_file_path)

def unlink_totally_unused_value_files(connection, repository_path):
    """
    * Find closed value file IDs who are not referenced 
    * In a db transaction:
        * delete rows from value_file table
        * unlink rows from disk 
          (and do not blow up on any files that do not exist, 
          since this action is outside of the database)
    """
    log = logging.getLogger("unlink_totally_unused_value_files")
    unused_value_file_ids = _list_unused_value_file_ids(connection)
    
    log.info(
        "found {0} unused value files".format(len(unused_value_file_ids))
    )

    connection.execute("begin", [])
    try:
        _unlink_value_files(
            connection, repository_path, unused_value_file_ids
        )
    except Exception:
        connection.rollback()
        raise
    else:
        connection.commit()

