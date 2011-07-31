# -*- coding: utf-8 -*-
"""
central_database_util.py

utility routines for the node local database
"""

from diyapi_tools.data_definitions import node_row_template

def node_rows(connection):
    """
    Retrieve information from the diy.node tble.
    This assumes that the table contains information only for
    the local cluster.
    """
    result = connection.fetch_all_rows("""
        select %s from diy_central.node
        order by node_number_in_cluster
    """ % (",".join(node_row_template._fields), )
    )
    if result is None:
        return None

    return [node_row_template._make(row) for row in result]

