from webob.dec import wsgify
from webob import exc
from webob import Response


class Application(object):
    @wsgify
    def __call__(self, req):
        return Response('hello world')
