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

def get_collections_for_avatar(connection, cluster_id, avatar_id):
    """
    return a list of tuples (collection_name, collection_id)
    listing all the colection the avatar has in a specific cluster
    """
    return connection.fetch_all_rows("""
        select id, name from diy_central.collection
        where avatar_id = %s
        and cluster_id = %s
        """, [avatar_id, cluster_id, ]
        )

