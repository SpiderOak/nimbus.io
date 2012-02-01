# -*- coding: utf-8 -*-
"""
versioned_collections
"""

import logging

from tools.database_connection import get_central_connection

_query = """
select id from nimbusio_central.collection
where versioning = true
"""

def get_versioned_collections():
    """
    return the set of all collection ids for which versioning is true
    """
    log = logging.getLogger("get_versioned_collections")
    versioned_collections = set()

    connection = get_central_connection()

    try:
        for (collection_id, ) in connection.fetch_all_rows(_query, []):
            versioned_collections.add(collection_id)

    finally:
        connection.close()

    log.info("found {0} versioned collectons".format(
        len(versioned_collections)
    ))

    return versioned_collections

