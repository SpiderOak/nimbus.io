# -*- coding: utf-8 -*-
"""
diyapi_web_server_main.py

Receives HTTP requests and distributes data to backend processes over amqp.
"""
import gevent
from gevent import monkey
monkey.patch_all()

import sys

from gevent import wsgi

from diyapi_web_server import Application


def make_wsgi_server():
    app = Application()
    host, port = ('', 8088)
    return wsgi.WSGIServer((host, port), app)


def main():
    make_wsgi_server().serve_forever()
    return 0


if __name__ == '__main__':
    sys.exit(main(*sys.argv))
