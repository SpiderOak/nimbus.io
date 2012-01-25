# -*- coding: utf-8 -*-
"""
test_url_discriminator.py

test recognizing the action required by URLs in the web server
"""
import os
import unittest

from tools.standard_logging import initialize_logging
from web_server.url_discriminator import parse_url, \
        action_list_collections, \
        action_create_collection, \
        action_delete_collection, \
        action_set_versioning, \
        action_list_versions, \
        action_space_usage, \
        action_archive_key, \
        action_list_keys, \
        action_retrieve_key, \
        action_retrieve_meta, \
        action_delete_key, \
        action_head_key, \
        action_list_conjoined, \
        action_start_conjoined, \
        action_finish_conjoined, \
        action_abort_conjoined, \
        action_list_upload_in_conjoined

_log_path = "%s/test_url_discriminator.log" % (
    os.environ["NIMBUSIO_LOG_DIR"], 
)

_valid_urls_with_actions = [
    (
        "GET", 
        "http://nimbus.io:8088/customers/test-user-name/collections",
        action_list_collections
    ),
    (
        "GET", 
        "https://nimbus.io/customers/test-user-name/collections",
        action_list_collections
    ),
    (
        "POST", 
        "https://nimbus.io/customers/test-user-name/collections?action=create&name=test-collection-name",
        action_create_collection
    ),
    (
        "DELETE", 
        "https://nimbus.io/customers/test-user-name/collections/test-collection-name",
        action_delete_collection
    ),
    (
        "POST", 
        "https://nimbus.io/customers/test-user-name/collections/test-collection-name?action=delete",
        action_delete_collection
    ),
    (
        "GET", 
        "https://nimbus.io/customers/test-user-name/collections/test-collection-name?action=space_usage",
        action_space_usage
    ),
    (
        "PUT", 
        "https://test-collection-name.nimbus.io/?versioning=True",
        action_set_versioning
    ),
    (
        "GET", 
        "https://test-collection-name.nimbus.io/?versions",
        action_list_versions
    ),
    (
        "POST", 
        "https://test-collection-name.nimbus.io/data/test-key",
        action_archive_key
    ),
    (
        "POST", 
        'http://test-collection-name.nimbus.io:8088/data/test-key?__nimbus_io__meta_key=pork',
        action_archive_key
    ),    
    (
        "POST", 
        'http://test-collection-name.nimbus.io:8088/data/test-key?conjoined_identifier=aaaaaaaaaaaaaaaaaaaaaa&conjoined_part=1',
        action_archive_key
    ),    
    (
        "GET", 
        "https://test-collection-name.nimbus.io/data/",
        action_list_keys
    ),
    (
        "GET", 
        "https://test-collection-name.nimbus.io/data/?xxx",
        action_list_keys
    ),
    (
        "GET", 
        "https://test-collection-name.nimbus.io/data/test-key",
        action_retrieve_key
    ),
    (
        "GET", 
        "https://test-collection-name.nimbus.io/data/test-key?action=meta",
        action_retrieve_meta
    ),
    (
        "DELETE", 
        "https://test-collection-name.nimbus.io/data/test-key",
        action_delete_key
    ),
    (
        "POST", 
        "https://test-collection-name.nimbus.io/data/test-key?action=delete",
        action_delete_key
    ),
    (
        "HEAD", 
        "https://test-collection-name.nimbus.io/data/test-key",
        action_head_key
    ),
    (
        "GET", 
        "https://test-collection-name.nimbus.io/conjoined/",
        action_list_conjoined,
    ),
    (
        "GET", 
        "https://test-collection-name.nimbus.io/conjoined/?max_conjoined=1000",
        action_list_conjoined,
    ),
    (
        "POST", 
        "https://test-collection-name.nimbus.io/conjoined/test-key?action=start",
        action_start_conjoined,
    ),
    (
        "POST", 
        "https://test-collection-name.nimbus.io/conjoined/test-key?action=finish&conjoined_identifier=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
        action_finish_conjoined,
    ),
    (
        "POST", 
        "https://test-collection-name.nimbus.io/conjoined/test-key?action=abort&conjoined_identifier=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
        action_abort_conjoined,
    ),
    (
        "GET", 
        "https://test-collection-name.nimbus.io/conjoined/test-key/xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx/",
        action_list_upload_in_conjoined,
    ),
]

def _valid_list_collections(match_object):
    return match_object.group("username") == "test-user-name"

def _valid_create_collection(match_object):
    return match_object.group("username") == "test-user-name" \
    and match_object.group("collection_name") == "test-collection-name"

def _valid_delete_collection(match_object):
    return match_object.group("username") == "test-user-name" \
    and match_object.group("collection_name") == "test-collection-name"

def _valid_set_versioning(match_object):
    return match_object.group("collection_name") == "test-collection-name" \
    and match_object.group("versioning") == "True"

def _valid_list_versions(match_object):
    return match_object.group("collection_name") == "test-collection-name"

def _valid_space_usage(match_object):
    if match_object.group("username") != "test-user-name":
        return False
    if match_object.group("collection_name") != "test-collection-name":
        return False
    try:
        prefix = match_object.group("prefix")
    except IndexError:
        prefix = ""
    return prefix in ["test-prefix", "", ]

def _valid_archive_key(match_object):
    return match_object.group("collection_name") == "test-collection-name" \
    and match_object.group("key") == "test-key"

def _valid_list_keys(match_object):
    return match_object.group("collection_name") == "test-collection-name"

def _valid_retrieve_key(match_object):
    return match_object.group("collection_name") == "test-collection-name" \
    and match_object.group("key") == "test-key"

def _valid_delete_key(match_object):
    return match_object.group("collection_name") == "test-collection-name" \
    and match_object.group("key") == "test-key"

def _valid_head_key(match_object):
    return match_object.group("collection_name") == "test-collection-name" \
    and match_object.group("key") == "test-key"

def _valid_retrieve_meta(match_object):
    return match_object.group("collection_name") == "test-collection-name" \
    and match_object.group("key") == "test-key"

def _valid_list_conjoined(match_object):
    return match_object.group("collection_name") == "test-collection-name"

def _valid_start_conjoined(match_object):
    return match_object.group("collection_name") == "test-collection-name"

def _valid_finish_conjoined(match_object):
    return match_object.group("collection_name") == "test-collection-name" \
            and match_object.group("conjoined_identifier") != ""

def _valid_abort_conjoined(match_object):
    return match_object.group("collection_name") == "test-collection-name" \
            and match_object.group("conjoined_identifier") != ""

def _valid_list_upload_in_conjoined(match_object):
    return match_object.group("collection_name") == "test-collection-name" \
            and match_object.group("conjoined_identifier") != ""

_match_object_dispatch_table = {
    action_list_collections     : _valid_list_collections,
    action_create_collection    : _valid_create_collection,
    action_delete_collection    : _valid_delete_collection,
    action_set_versioning       : _valid_set_versioning,
    action_list_versions        : _valid_list_versions,
    action_space_usage          : _valid_space_usage,
    action_archive_key          : _valid_archive_key,
    action_list_keys            : _valid_list_keys,
    action_retrieve_key         : _valid_retrieve_key,
    action_delete_key           : _valid_delete_key,
    action_head_key             : _valid_head_key,
    action_retrieve_meta        : _valid_retrieve_meta,
    action_list_conjoined       : _valid_list_conjoined,
    action_start_conjoined      : _valid_start_conjoined,
    action_finish_conjoined     : _valid_finish_conjoined,
    action_abort_conjoined      : _valid_abort_conjoined,
    action_list_upload_in_conjoined     : _valid_list_upload_in_conjoined,
}

class TestUrlDiscriminator(unittest.TestCase):
    """test the customer authentication process"""

    def setUp(self):
        self.tearDown()

    def tearDown(self):
        pass

    def test_invalid_url(self):
        """try a URL we cant parse"""
        result = parse_url("GET", "pork")
        self.assertEqual(result, None)

    def test_valid_urls(self):
        """run through all the URLs we should be able to parse"""
        for method, url, expected_action in _valid_urls_with_actions:
            result = parse_url(method, url)
            self.assertNotEqual(result, None, (method, url, ))
            action, match_object = result
            self.assertEqual(
                action, expected_action, 
                (action, expected_action, method, url, )
            )
            self.assertTrue(
                _match_object_dispatch_table[action](match_object),
                (method, url,)
            )

if __name__ == "__main__":
    initialize_logging(_log_path)
    unittest.main()


