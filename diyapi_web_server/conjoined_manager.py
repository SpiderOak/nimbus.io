# -*- coding: utf-8 -*-
"""
conjoined_manager.py

functions for conjoined archive data
"""
import logging
import uuid

import psycopg2

def list_conjoined_archives(connection, collection_id):
    """
    get a list of tuples for all active conjoiined archives
    """
    return connection.fetch_all_rows("""
        select identifier, key from diy.conjoined where
        collection_id = %s and abort_timestamp is null 
        and complete_timestamp is null
        """.strip(), [collection_id, ]
    )

def start_conjoined_archive(connection, collection_id, key):
    """
    start a new conjoined archive
    """
    log = logging.getLogger("start_conjoined_archive")
    # if we already have an active archive for this key, use that
    result = connection.fetch_one_row("""
        select identifier from diy_central.conjoined
        where collection_id = %s and key = %s
        """, [collection_id, key, ]
    )
    if result is not None:
        identifier = uuid.UUID(str(result[0]))
        log.warn("using existing conjoined archive %r %r %r" % (
            collection_id, key, identifier.hex,
        ))
        return identifier.hex

    identifier = uuid.uuid1()

    log.info("creating conjoined archive %r %r %r" % (
        collection_id, key, identifier.hex,
    ))

    connection.execute("""
        insert into diy_central.conjoined (collection_id, identifier, key) 
        values (%s, %s, %s);""", 
        [collection_id, psycopg2.Binary(identifier.bytes), key]
    )

    return identifier.hex

def abort_conjoined_archive(connection, identifier_hex):
    """
    mark a conjoined archive as aborted
    """
    pass

def finish_conjoined_archive(connection, identifier_hex):
    """
    finish a conjoined archive
    """
    pass

