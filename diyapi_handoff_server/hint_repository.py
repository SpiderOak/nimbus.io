# -*- coding: utf-8 -*-
"""
handoff_repository.py

A module for store and retrieving handoff hints
"""
from collections import namedtuple
import logging
import os
import os.path
import sqlite3

factory =  namedtuple(
    "HandoffHint", [
        "node_name",
        "avatar_id",
        "timestamp", 
        "key",
        "version_number",
        "segment_number", 
        "action",
        "server_node_names",
    ]
)

_repository_path = os.environ["DIYAPI_REPOSITORY_PATH"]
_database_name = "handoff_hints.dat"
_database_path = os.path.join(_repository_path, _database_name)
_schema = """
create table hints (
    id integer primary key autoincrement,
    node_name,
    avatar_id int4 not null,
    timestamp timestamp not null, 
    key text not null,
    version_number int4 not null,
    segment_number int2 not null,
    action text not null,
    server_node_names text not null
);
create unique index hints_idx 
on hints (node_name, avatar_id, timestamp, key);
""".strip()

_store = """
insert into hints (
    node_name, 
    avatar_id, 
    timestamp, 
    key, 
    version_number, 
    segment_number,
    action,
    server_node_names
)
values (?, ?, ?, ?, ?, ?, ?, ?);
""".strip()

_oldest_row_for_node_name = """
select node_name, avatar_id, timestamp, key, version_number, segment_number,
action, server_node_names 
from hints
where node_name = ?
order by timestamp
limit 1
""".strip()

_purge = """
delete from hints 
where node_name = ? 
and avatar_id = ?
and timestamp = ? 
and key = ? 
and version_number = ?
and segment_number = ?
and action = ?
""".strip()

def _connect_to_database():
    """connect to the stack garbage database, creating it if neccessary"""
    need_schema = not os.path.exists(_database_path)
    connection = sqlite3.connect(_database_path)
    connection.text_factory = str # always return bytestrings (not unicode)
    if need_schema:
        connection.executescript(_schema)
    return connection

class HintRepository(object):
    """wrap a repository for storing and retrieving handoff hints"""

    def __init__(self):
        self._log = logging.getLogger("HintRepository")
        self._connection = _connect_to_database()
        
    def close(self):
        """close the database"""
        self._connection.close()

    def store(
        self, 
        node_name, 
        avatar_id, 
        timestamp,
        key, 
        version_number, 
        segment_number,
        action,
        server_node_names
    ):
        """
        store a hint to handoff this archive to dest node-name when its
        data_writer announces its presence.
        """
        self._log.debug("store: %s %s %s" % (node_name, avatar_id, key, ))
        with self._connection:
            cursor = self._connection.cursor()
            cursor.execute(
                _store, (
                    node_name,
                    avatar_id, 
                    timestamp, 
                    key, 
                    version_number, 
                    segment_number,
                    action,
                    " ".join(server_node_names)
                )
            )
            cursor.close()

    def next_hint(self, node_name):
        """
        retrieve the next avaialble hint for the exchange.
        returns a HandoffHint object or None
        """
        cursor = self._connection.cursor()
        cursor.execute(_oldest_row_for_node_name, (node_name, ))
        row = cursor.fetchone()
        cursor.close()
        self._log.debug("next_hint(%s) found %r" % (node_name, row, ))
        if row is None:
            return None
        return factory._make(row)

    def purge_hint(self, handoff_hint):
        """
        remove the database entry for a HandoffHint object
        """
        self._log.debug("purge_hint: %s" % (handoff_hint, ))
        with self._connection:
            cursor = self._connection.cursor()
            cursor.execute(
                _purge, (
                    handoff_hint.node_name, 
                    handoff_hint.avatar_id,
                    handoff_hint.timestamp, 
                    handoff_hint.key, 
                    handoff_hint.version_number, 
                    handoff_hint.segment_number,
                    handoff_hint.action,
                )
            )
            cursor.close()

