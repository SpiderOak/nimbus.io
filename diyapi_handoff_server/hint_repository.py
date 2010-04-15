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
        "timestamp", 
        "exchange",
        "avatar_id",
        "key",
        "version_number",
        "segment_number", 
    ]
)

_repository_path = os.environ["PANDORA_REPOSITORY_PATH"]
_database_name = "handoff_hints.dat"
_database_path = os.path.join(_repository_path, _database_name)
_schema = """
create table hints (
    id integer primary key autoincrement,
    exchange text not null,
    timestamp timestamp not null, 
    avatar_id int4 not null,
    key text not null,
    version_number int4 not null,
    segment_number int2 not null
);
create unique index hints_idx 
on hints (exchange, timestamp, avatar_id, key, version_number, segment_number);
""".strip()

_store = """
insert into hints (
    exchange, timestamp, avatar_id, key, version_number, segment_number
)
values (?, ?, ?, ?, ?, ?)
""".strip()

_oldest_row_for_exchange = """
select timestamp, exchange, avatar_id, key, version_number, segment_number 
from hints
where exchange = ?
order by timestamp
limit 1
""".strip()

_purge = """
delete from hints 
where exchange = ? 
and timestamp = ? 
and avatar_id = ?
and key = ? 
and version_number = ?
and segment_number = ?
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
        exchange, 
        timestamp, 
        avatar_id, 
        key, 
        version_number, 
        segment_number
    ):
        """
        store a hint to handoff this archive to dest exchange when its
        data_writer announces its presence.
        """
        with self._connection:
            cursor = self._connection.cursor()
            cursor.execute(
                _store, (
                    exchange, 
                    timestamp, 
                    avatar_id, 
                    key, 
                    version_number, 
                    segment_number, 
                )
            )
            cursor.close()

    def next_hint(self, exchange):
        """
        retrieve the next avaialble hint for the exchange.
        returns a HandoffHint object or None
        """
        cursor = self._connection.cursor()
        cursor.execute(_oldest_row_for_exchange, (exchange, ))
        row = cursor.fetchone()
        cursor.close()
        if row is None:
            return None
        return factory._make(row)

    def purge_hint(self, handoff_hint):
        """
        remove the database entry for a HandoffHint object
        """
        with self._connection:
            cursor = self._connection.cursor()
            cursor.execute(
                _purge, (
                    handoff_hint.exchange, 
                    handoff_hint.timestamp, 
                    handoff_hint.avatar_id,
                    handoff_hint.key, 
                    handoff_hint.version_number, 
                    handoff_hint.segment_number,
                )
            )
            cursor.close()

