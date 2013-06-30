#!/usr/bin/env python2.7 

"""
only for testing!
convenience utility for removing data from node local database created by
motoboto test and motoboto benchmark users.
"""

from tools.database_connection import \
    get_central_connection, get_node_local_connection

_test_collections_query = """
select id 
from nimbusio_central.collection 
where customer_id in (select customer_id 
                      from nimbusio_central.customer 
                      where username='motoboto-test-01' 
                      or username ~ E'^motoboto-benchmark-[0-9]{3}' )
""".strip()

_delete_test_collections_data = [
"""
delete from nimbusio_node.segment 
where collection_id in (
    select collection_id from tmp_motoboto_collection_ids)
""".strip(),

"""
delete from nimbusio_node.garbage_segment_conjoined_recent
where collection_id in (
    select collection_id from tmp_motoboto_collection_ids)
""".strip(),

"""
delete from nimbusio_node.garbage_segment_conjoined_old
where collection_id in (
    select collection_id from tmp_motoboto_collection_ids)
""".strip(),

"""
delete from nimbusio_node.damaged_segment
where collection_id in (
    select collection_id from tmp_motoboto_collection_ids)
""".strip(),

"""
delete from nimbusio_node.segment_sequence
where collection_id in (
    select collection_id from tmp_motoboto_collection_ids)
""".strip(),

"""
delete from nimbusio_node.conjoined
where collection_id in (
    select collection_id from tmp_motoboto_collection_ids)
""".strip(),

"""
delete from nimbusio_node.meta
where collection_id in (
    select collection_id from tmp_motoboto_collection_ids)
""".strip(),

]

def delete_all_motoboto_test_segments():
    central_conn = get_central_connection()
    local_conn = get_node_local_connection()
    collection_id_rows = central_conn.fetch_all_rows(
        _test_collections_query, [])
    central_conn.close()

    local_conn.begin_transaction()
    local_conn.execute(
        "create temp table tmp_motoboto_collection_ids (id int4 not null)", [])
    for row in collection_id_rows:
        local_conn.execute(
            "insert into tmp_motoboto_collection_ids values (%s)", row)

    for query in _delete_test_collections_data:
        rowcount = local_conn.execute(query, [])
        if rowcount:
            print "Deleted %s via %s" % ( rowcount, query.split("\n", 1)[0], )

    local_conn.commit()

if __name__ == "__main__":
    delete_all_motoboto_test_segments()
