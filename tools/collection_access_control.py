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

"""
if True, GET and HEAD requests for objects do not
require authentication
"""
allow_unauth_read = "allow_unauth_read"

"""
if True, PUT and POST requests for objects do not
require authentication
"""
allow_unauth_write = "allow_unauth_write"

"""
if True, LISTMATCH does not require authentication
"""
allow_unauth_list = "allow_unauth_list"

"""
if True, DELETE does not require authentication
"""
allow_unauth_delete = "allow_unauth_delete"

"""
if not null, must be a list of strings describing IPv4
addresses of the form "W.X.Y.Z" or netblocks of the
form "W.X.Y.Z/N" . Any request not originating from
within a listed netblock will be failed with HTTP
403. 
"""
ipv4_whitelist = "ipv4_whitelist"

"""
if not null, must be a list of strings.  Any request
that does not include a HTTP Referer (sic) header
prefixed with one the strings in the whitelist will be
rejected with HTTP 403.
The prefix refers to the part immediately after the
schema.  For example, if unauth_referrer_whitelist was
set to :
[ "example.com/myapp" ]
then a request with a Referer header of
http://example.com/myapp/login would be allowed.
"""
unauth_referrer_whitelist = "unauth_referrer_whitelist"

"""
the locations property allows a way to specify more
fine grained access controls.  if present, it must be
a list of objects, and the first object found to be
matching the request will be used to define the
access controls for the request.  if no match is
found, the default settings specified for the
collection in general (i.e. outside the 'locations'
property) will be used.

each object in the locations list must have either a
"prefix" or a "regexp" property.  this will be used
to see if an incoming request is subject to the
objects access controls.  each object in the list may
then have zero or more of the above properties
allowed for access control (i.e. allow_unauth_read,
ipv4_whitelist, etc.) 

In this way it is possible to have fine grained
access controls over different parts of a collection.
For example: 
{   
  "allow_unauth_read": false, 
  "ipv4_whitelist": null, 
  "prefix": "/abc"
},  
{   
  "allow_unauth_read": false, 
  "ipv4_whitelist": null, 
  "prefix": "/def"
}  
""" 
locations = "locations"

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

