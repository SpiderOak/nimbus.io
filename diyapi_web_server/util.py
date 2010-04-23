# -*- coding: utf-8 -*-
"""
util.py

Utility functions for web server
"""
import re


_HOST_USERNAME_RE = re.compile(r'([^.]+)\.diy\.spideroak\.com(?::\d+)?$')


def get_username_from_req(req):
    m = _HOST_USERNAME_RE.match(req.host)
    if m:
        return m.group(1).lower()
