# -*- coding: utf-8 -*-
"""
url_discriminator.py

Parse incoming URLs to identify their actions
See the nimbus.io Developer's Guide 

https://nimbus.io/customers/<username>
https://nimbus.io/customers/<username>/collections
https://<collection name>.numbus.io/data/
https://<collection name>.nimbus.io/data/<key>
"""
import re

action_list_collections = "list-collections"
action_create_collection = "create-collection"
action_delete_collection = "delete-collection"
action_space_usage = "space-usage"
action_archive_key = "archive-key"
action_list_keys = "list-keys"
action_retrieve_key = "retrieve-key"
action_retrieve_meta = "retrieve-meta"
action_delete_key = "delete-key"
action_head_key = "head-key"

_list_collections_re = re.compile(
    r"^http(s?)://nimbus\.io(:\d+)?/customers/(?P<username>[a-zA-Z0-9-]+)/collections$"
)

_create_collection_re = re.compile(
    r"^http(s?)://nimbus\.io(:\d+)?/customers/(?P<username>[a-zA-Z0-9-]+)/collections\?action=create\&name=(?P<collection_name>[a-zA-Z0-9-]+)$"
)

_delete_collection1_re = re.compile(
    r"^http(s?)://nimbus\.io(:\d+)?/customers/(?P<username>[a-zA-Z0-9-]+)/collections/(?P<collection_name>[a-zA-Z0-9-]+)$"
)
_delete_collection2_re = re.compile(
    r"^http(s?)://nimbus\.io(:\d+)?/customers/(?P<username>[a-zA-Z0-9-]+)/collections/(?P<collection_name>[a-zA-Z0-9-]+)\?action=delete$"
)

_space_usage_re = re.compile(
    r"^http(s?)://nimbus\.io(:\d+)?/customers/(?P<username>[a-zA-Z0-9-]+)/collections/(?P<collection_name>[a-zA-Z0-9-]+)\?action=space_usage$"
)

_archive_key_re = re.compile(
    r"^http(s?)://(?P<collection_name>[a-zA-Z0-9-]+)\.nimbus\.io(:\d+)?/data/(?P<key>\S+?)(\?.*)?$"
)

_list_keys_re = re.compile(
    r"^http(s?)://(?P<collection_name>[a-zA-Z0-9-]+)\.nimbus\.io(:\d+)?/data(/|\?prefix=(?P<prefix>\S+?))$"
)

_retrieve_meta_re = re.compile(
    r"^http(s?)://(?P<collection_name>[a-zA-Z0-9-]+)\.nimbus\.io(:\d+)?/data/(?P<key>\S+?)\?action=meta$"
)
_retrieve_key_re = re.compile(
    r"^http(s?)://(?P<collection_name>[a-zA-Z0-9-]+)\.nimbus\.io(:\d+)?/data/(?P<key>\S+?)$"
)

_delete_key1_re = re.compile(
    r"^http(s?)://(?P<collection_name>[a-zA-Z0-9-]+)\.nimbus\.io(:\d+)?/data/(?P<key>\S+?)$"
)
_delete_key2_re = re.compile(
    r"^http(s?)://(?P<collection_name>[a-zA-Z0-9-]+)\.nimbus\.io(:\d+)?/data/(?P<key>\S+?)\?action=delete$"
)

_head_key_re = re.compile(
    r"^http(s?)://(?P<collection_name>[a-zA-Z0-9-]+)\.nimbus\.io(:\d+)?/data/(?P<key>\S+)$"
)

# note that order is significant here, 
# specifically _archive_key_re has a grab bag ?.* that will pick up
# ?action=delete, or ?action=metaso we need to check for those first
_regex_by_method = {
    "GET"   : [
        (_list_collections_re, action_list_collections, ),
        (_space_usage_re, action_space_usage, ),
        (_list_keys_re, action_list_keys, ),
        (_retrieve_meta_re, action_retrieve_meta, ),
        (_retrieve_key_re, action_retrieve_key, ),
    ],
    "POST"  : [
        (_create_collection_re, action_create_collection, ),
        (_delete_collection2_re, action_delete_collection, ),
        (_delete_key2_re, action_delete_key, ),
        (_archive_key_re, action_archive_key, ),
    ],
    "DELETE"  : [
        (_delete_collection1_re, action_delete_collection, ),
        (_delete_key1_re, action_delete_key, ),
    ],
    "HEAD"  : [
        (_head_key_re, action_head_key, ),
    ],
}

# Customer Account
# TODO what do we do for the customer account
# https://nimbus.io/customers/<username>

def parse_url(method, url):
    """
    Identify the action reqired by the URL
    Return an action tag and the regular expression match object
    """
    method = method.upper()
    if method not in _regex_by_method:
        return None

    for regex, action in _regex_by_method[method]:
        match_object = regex.match(url)
        if match_object is not None:
            return action, match_object

    return None

