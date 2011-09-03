# -*- coding: utf-8 -*-
"""
test library routines for accessing the central database.
This also tests the validity of the central database.
"""
from diyapi_tools.database_connection import get_central_connection
from diyapi_web_server.central_database_util import get_cluster_row, \
    get_node_rows, \
    get_collections_for_collection  

if __name__ =="__main__":
    connection = get_central_connection()
    cluster_row = get_cluster_row(connection)
    print "cluster_row", cluster_row
    node_rows = get_node_rows(connection, cluster_row.id)
    print "node_rows"
    for node_row in node_rows:
        print "    ", node_row
    collections = get_collections_for_collection(
        connection, cluster_row.id, 1001
    )
    print "collections"
    for collection_name, collection_id in collections:
        print "    ", collection_name, collection_id
    connection.close()

