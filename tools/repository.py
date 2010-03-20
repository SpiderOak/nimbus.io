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
    path = os.path.join(_repository_path, "diyapi")
    if not os.path.exists(path):
        os.makedirs(path)
    return path

def content_database_path(avatar_id):
    """return the path to the diyapi database"""
    return os.path.join(_diyapi_dir(avatar_id), "contents.db")

