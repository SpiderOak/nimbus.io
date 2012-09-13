# -*- coding: utf-8 -*-
"""
collection_access_control.py

Ticket #43 Implement access_control properties for collections
"""
import logging

read_access = 1
write_access = 2
list_access = 3
delete_access = 4

access_allowed = 1
access_requires_password_authentication = 2
access_forbidden = 3

def check_access_control(access_type, request, access_control):
    """
    return an integer result
        * access_allowed
        * access_requires_password_authentication
        * access_forbidden

    action: the type of access requested
        * read_access
        * write_access
        * list_access
        * delete_access

    request
        WebOb Request object
        http://docs.webob.org/en/latest/reference.html#id1

    access_control
        dictionary created by unpickling the access_control column
        of nimbusio_central.collections. 
        If the column is null, submit and empty dictionary {}
    """
    # if no special access control is specified, we must authenticate
    if len(access_control) == 0:
        return access_requires_password_authentication

    # if no access_control clause applies to this request, we must authenticate
    return access_requires_password_authentication

