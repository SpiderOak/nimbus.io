# -*- coding: utf-8 -*-
"""
unified_id_factory.py

generate unique ids
"""
_max_shard_id  = 0b1111111111111 # 13 bits for shard id
_shard_schema_template = """
drop schema if exists nimbusio_shard_{0:0>5} cascade;
create schema nimbusio_shard_{0:0>5};
set search_path to nimbusio_shard_{0:0>5}, public;

create sequence nimbusio_shard_{0:0>5}.base_seq;

CREATE OR REPLACE FUNCTION nimbusio_shard_{0:0>5}.next_id(OUT result bigint) AS $$
DECLARE
    our_epoch bigint := 1314220021721;
    seq_id bigint;
    now_millis bigint;
    shard_id int := {0};
BEGIN
    SELECT nextval('nimbusio_shard_{0:0>5}.base_seq') % 1024 INTO seq_id;

    SELECT FLOOR(EXTRACT(EPOCH FROM clock_timestamp()) * 1000) INTO now_millis;
    result := (now_millis - our_epoch) << 23;
    result := result | (shard_id << 10);
    result := result | (seq_id);
END;
$$ LANGUAGE PLPGSQL;
"""

class UnifiedIDFactory(object):
    """
    Based on the instagram sharded ids

    http://instagram-engineering.tumblr.com/post/10853187575/sharding-ids-at-instagram

    At startup:
     * create a schema based on shard id
     * create next_id table
     * install the next_id function
    """
    def __init__(self, connection, shard_id):
        if shard_id > _max_shard_id:
            raise ValueError("shard_id {0} exceeds {1}".format(
                shard_id, _max_shard_id
            ))
        self._shard_id = shard_id
        self._connection = connection
        shard_schema = _shard_schema_template.format(shard_id)
        self._connection.execute(shard_schema)

    def __iter__(self):
        return self

    def next(self):
        """
        return the next id, by calling the postgres next_id function
        """
        (next_id, ) = self._connection.fetch_one_row(
            "select next_id from nimbusio_shard_{0:0>5}.next_id()".format(
                self._shard_id
            ), 
            []
        )

        return next_id

