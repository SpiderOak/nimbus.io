# -*- coding: utf-8 -*-
"""
repository.py

common code for accessing the storage repository
"""
import os
import os.path

_repository_path = os.environ["PANDORA_REPOSITORY_PATH"]

def _diyapi_dir(avatar_id):
    """return the base siyapi path, and assure that it exists"""
    path = os.path.join(_repository_path, str(avatar_id), "diyapi")
    if not os.path.exists(path):
        os.makedirs(path)
    return path

def content_database_path(avatar_id):
    """return the path to the diyapi database"""
    return os.path.join(_diyapi_dir(avatar_id), "contents.db")

def content_input_path(avatar_id, key):
    """return the path to the work area where content is written"""
    path = os.path.join(_diyapi_dir(avatar_id), "in")
    if not os.path.exists(path):
        os.mkdir(path)
    return os.path.join(path, key)

def _hash_dirs(key):
    """generate one digit directory names"""
    hash_value = hash(key) % 10000
    for _ in range(4):
        yield str(hash_value % 10)
        hash_value /= 10

def content_path(avatar_id, key):
    """returns the path to the permanent location of content"""
    dir4, dir3, dir2, dir1 = _hash_dirs(key)
    path = os.path.join(_diyapi_dir(avatar_id), dir1, dir2, dir3, dir4)
    if not os.path.exists(path):
        os.makedirs(path)
    return os.path.join(path, key)

