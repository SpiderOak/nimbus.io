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
action_archive_key = "archive-key"
action_delete_key = "delete-key"
action_start_conjoined = "start-conjoined"
action_finish_conjoined = "finish-conjoined"
action_abort_conjoined = "abort-conjoined"

_service_domain = os.environ.get("NIMBUS_IO_SERVICE_DOMAIN", "nimbus.io")
_re_service_domain = _service_domain.replace(".", "\.")

_ping_re = re.compile(
    r"^http://.*/ping$"
)

_archive_key_re = re.compile(
    r"^http(s?)://(?P<collection_name>[a-zA-Z0-9-]+)\." + _re_service_domain + r"(:\d+)?/data/(?P<key>\S+?)(\?.*)?$"
)

_delete_key1_re = re.compile(
    r"^http(s?)://(?P<collection_name>[a-zA-Z0-9-]+)\." + _re_service_domain + r"(:\d+)?/data/(?P<key>\S+?)(\?.*)?$"
)
_delete_key2_re = re.compile(
    r"^http(s?)://(?P<collection_name>[a-zA-Z0-9-]+)\." + _re_service_domain + r"(:\d+)?/data/(?P<key>\S+?)\?action=delete(\&.*)?$"
)

_start_conjoined_re = re.compile(
    r"^http(s?)://(?P<collection_name>[a-zA-Z0-9-]+)\." + _re_service_domain + r"(:\d+)?/conjoined/(?P<key>\S+?)\?action=start$"
)

_finish_conjoined_re = re.compile(
    r"^http(s?)://(?P<collection_name>[a-zA-Z0-9-]+)\." + _re_service_domain + r"(:\d+)?/conjoined/(?P<key>\S+?)\?action=finish\&conjoined_identifier=(?P<conjoined_identifier>\S+)$"
)

_abort_conjoined_re = re.compile(
    r"^http(s?)://(?P<collection_name>[a-zA-Z0-9-]+)\." + _re_service_domain + r"(:\d+)?/conjoined/(?P<key>\S+?)\?action=abort\&conjoined_identifier=(?P<conjoined_identifier>\S+)$"
)

# note that order is significant here, 
# specifically _archive_key_re has a grab bag ?.* that will pick up
# ?action=delete, or ?action=meta so we need to check for those first
_regex_by_method = {
    "GET"   : [
        (_ping_re, action_respond_to_ping, ),
    ],
    "POST"  : [
        (_delete_key2_re, action_delete_key, ),
        (_archive_key_re, action_archive_key, ),
        (_start_conjoined_re, action_start_conjoined, ),
        (_finish_conjoined_re, action_finish_conjoined, ),
        (_abort_conjoined_re, action_abort_conjoined, ),
    ],
    "PUT"   : [
    ],
    "DELETE"  : [
        (_delete_key1_re, action_delete_key, ),
    ],
    "HEAD"  : [
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

