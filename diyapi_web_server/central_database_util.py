# -*- coding: utf-8 -*-
"""
central_database_util.py

utility routines for the node local database
"""
import os

from diyapi_tools.data_definitions import cluster_row_template, \
        node_row_template

_cluster_name = os.environ["SPIDEROAK_MULTI_CLUSTER_NAME"]

def get_cluster_row(connection):
    result = connection.fetch_one_row("""
        select %s from diy_central.cluster
        where name = %%s
    """ % (",".join(cluster_row_template._fields), ),
    [_cluster_name, ]
    )
    if result is None:
        return None

    return cluster_row_template._make(result)

def get_node_rows(connection, cluster_id):
    """
    Retrieve information from the diy_central.node table.
    """
    result = connection.fetch_all_rows("""
        select %s from diy_central.node
        where  cluster_id = %%s
        order by node_number_in_cluster
    """ % (",".join(node_row_template._fields), ),
    [cluster_id, ]
    )
    if result is None:
        return None

    return [node_row_template._make(row) for row in result]

