# -*- coding: utf-8 -*-
"""
advisory_lock.py

manage a postgresql advisory lock

see nimbus.io/sql/locks.txt
"""
from collections import namedtuple
import contextlib
import os

class AdvisoryLockError(Exception):
    pass
class AdvisoryUnlockError(Exception):
    pass

_cluster_name = os.environ["NIMBUSIO_CLUSTER_NAME"]
_lock_token = namedtuple("LockToken", ["key1", "key2"])

_central_database_lock_key1 = {"redis_stats_collector" : 779838032, } 

def acquire_advisory_lock(connection, lock_name):
    """
    acquire a named advisory lock
    if successful, return a token that can be used to release the lock
    if unsuccessful, raise AdvisoryLockError
    """
    key1 = _central_database_lock_key1[lock_name]
    (data_center_id, ) = connection.fetch_one_row("""
        select data_center_id from nimbusio_central.cluster
        where name = %s""", [_cluster_name, ])
    (ok, ) = connection.fetch_one_row("select pg_try_advisory_lock(%s, %s)",
                                      [key1, data_center_id])
    if ok:
        return _lock_token(key1=key1, key2=data_center_id)

    raise AdvisoryLockError(lock_name)

def release_advisory_lock(connection, lock_token):
    """
    release a named advisory lock
    if successful, return a token that can be used to release the lock
    if unsuccessful, raise AdvisoryLockError
    """
    (ok, ) = connection.fetch_one_row("select pg_advisory_unlock(%s, %s)",
                                      [lock_token.key1, lock_token.key2])
    if not ok:
        raise AdvisoryUnlockError("{0} {1}".format(lock_token.key1, 
                                                  lock_token.key2))

@contextlib.contextmanager
def advisory_lock(connection, lock_name):
    """
    context manager for advisory lock
    usage:
        with advisory_lock(connection, lock_name):
            ...
    """
    lock_token = acquire_advisory_lock(connection, lock_name)
    yield
    release_advisory_lock(connection, lock_token)

