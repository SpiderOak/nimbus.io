# -*- coding: utf-8 -*-
"""
url_discriminator.py

Parse incoming URLs to identify their actions
See the nimbus.io Developer's Guide 

http://<unified-id>/<conjoined-part>
"""
import re

action_respond_to_ping = "respond-to-ping"
action_retrieve_key = "retrieve-key"

_ping_re = re.compile(
    r"^http://.*/ping$"
)

_retrieve_key_re = re.compile(
    r"^http(s?)://(?P<unified-id>\d+)/(?p<conjoined-part>\d+)$"
)

_regex_by_method = {
    "GET"   : [
        (_ping_re, action_respond_to_ping, ),
        (_retrieve_key_re, action_retrieve_key, ),
    ],
    "POST"  : [ ],
    "PUT"   : [ ],
    "DELETE": [ ],
    "HEAD"  : [ ],
}

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

