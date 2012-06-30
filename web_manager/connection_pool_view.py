# -*- coding: utf-8 -*-
"""
connection_pool_view.py

A simmple View that declares a class variable to provide access to the
connection pool to all derived classes.
"""

import flask.views

class ConnectionPoolView(flask.views.View):
    connection_pool = None

