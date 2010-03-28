# -*- coding: utf-8 -*-
"""
application.py

The diyapi wsgi application
"""
from webob.dec import wsgify
from webob import exc
from webob import Response


class Application(object):
    @wsgify
    def __call__(self, req):
        return Response('%s %s (%d)' % (req.method, req.path, req.content_length))
