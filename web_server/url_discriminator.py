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
import os

action_respond_to_ping = "respond-to-ping"
action_list_versions = "list-versions"
action_space_usage = "space-usage"
action_list_keys = "list-keys"
action_retrieve_key = "retrieve-key"
action_retrieve_meta = "retrieve-meta"
action_head_key = "head-key"
action_list_conjoined = "list-conjoined"
action_list_upload_in_conjoined = "list-uploads-in-conjoined"

_service_domain = os.environ.get("NIMBUS_IO_SERVICE_DOMAIN", "nimbus.io")
_re_service_domain = _service_domain.replace(".", "\.")

_ping_re = re.compile(
    r"^http://.*/ping$"
)

_list_versions_re = re.compile(
    r"^http(s?)://(?P<collection_name>[a-zA-Z0-9-]+)\." + _re_service_domain + r"(:\d+)?/\?versions(\&.*)?$"
)

_space_usage_re = re.compile(
    r"^http(s?)://" + _re_service_domain + r"(:\d+)?/customers/(?P<username>[a-zA-Z0-9-]+)/collections/(?P<collection_name>[a-zA-Z0-9-]+)\?action=space_usage$"
)

_list_keys_re = re.compile(
    r"^http(s?)://(?P<collection_name>[a-zA-Z0-9-]+)\." + _re_service_domain + r"(:\d+)?/data/(\?.*)?$"
)

_retrieve_meta_re = re.compile(
    r"^http(s?)://(?P<collection_name>[a-zA-Z0-9-]+)\." + _re_service_domain + r"(:\d+)?/data/(?P<key>\S+?)\?action=meta$"
)
_retrieve_key_re = re.compile(
    r"^http(s?)://(?P<collection_name>[a-zA-Z0-9-]+)\." + _re_service_domain + r"(:\d+)?/data/(?P<key>\S+?)(\?.*)?$"
)

_head_key_re = re.compile(
    r"^http(s?)://(?P<collection_name>[a-zA-Z0-9-]+)\." + _re_service_domain + r"(:\d+)?/data/(?P<key>\S+)$"
)

_list_conjoined_re = re.compile(
    r"^http(s?)://(?P<collection_name>[a-zA-Z0-9-]+)\." + _re_service_domain + r"(:\d+)?/conjoined/(\?.*)?$"
)

_list_upload_in_conjoined_re = re.compile(
    r"^http(s?)://(?P<collection_name>[a-zA-Z0-9-]+)\." + _re_service_domain + r"(:\d+)?/conjoined/(?P<key>\S+?)/(?P<conjoined_identifier>\S+)/$"
)

# note that order is significant here, 
# specifically _archive_key_re has a grab bag ?.* that will pick up
# ?action=delete, or ?action=meta so we need to check for those first
_regex_by_method = {
    "GET"   : [
        (_ping_re, action_respond_to_ping, ),
        (_space_usage_re, action_space_usage, ),
        (_list_versions_re, action_list_versions, ),
        (_list_keys_re, action_list_keys, ),
        (_retrieve_meta_re, action_retrieve_meta, ),
        (_retrieve_key_re, action_retrieve_key, ),
        (_list_conjoined_re, action_list_conjoined, ),
        (_list_upload_in_conjoined_re, action_list_upload_in_conjoined, ),
    ],
    "POST"  : [ ],
    "PUT"   : [ ],
    "DELETE": [ ],
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

