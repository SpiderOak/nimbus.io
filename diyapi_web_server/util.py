# -*- coding: utf-8 -*-
"""
util.py

Utility functions for web server
"""
from itertools import izip
import re


_HOST_USERNAME_RE = re.compile(r'([^.]+)\.diy\.spideroak\.com(?::\d+)?$')


def get_username_from_req(req):
    m = _HOST_USERNAME_RE.match(req.host)
    if m:
        return m.group(1).lower()

def sec_str_eq(str1, str2):
    "efficient constant time string comparison for arbirtrary strings"
    if not len(str1) == len(str2):
        return False
    
    match_count = 0
    for a, b in izip(str1, str2):
        match_count += 1 if a == b else 0
            
    return match_count == len(str1)

