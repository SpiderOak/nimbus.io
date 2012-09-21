# -*- coding: utf-8 -*-
"""
collection_access_control.py

Ticket #43 Implement access_control properties for collections
"""
from copy import deepcopy
import json
import logging
import re
from urlparse import urlparse

import ipaddr

class AccessControlError(Exception):
    pass
class AccessControlCleanseError(AccessControlError):
    pass
class InvalidAccessControl(AccessControlError):
    pass

read_access = 1
write_access = 2
list_access = 3
delete_access = 4

access_allowed = 1
access_requires_password_authentication = 2
access_forbidden = 3

version = "version"

#if True, GET and HEAD requests for objects do not
#require authentication
allow_unauth_read = "allow_unauth_read"

#if True, PUT and POST requests for objects do not
#require authentication
allow_unauth_write = "allow_unauth_write"

#if True, LISTMATCH does not require authentication
allow_unauth_list = "allow_unauth_list"

#if True, DELETE does not require authentication
allow_unauth_delete = "allow_unauth_delete"

#if not null, must be a list of strings describing IPv4
#addresses of the form "W.X.Y.Z" or netblocks of the
#form "W.X.Y.Z/N" . Any request not originating from
#within a listed netblock will be failed with HTTP
#403. 
ipv4_whitelist = "ipv4_whitelist"

#if not null, must be a list of strings.  Any request
#that does not include a HTTP Referer (sic) header
#prefixed with one the strings in the whitelist will be
#rejected with HTTP 403.
#The prefix refers to the part immediately after the
#schema.  For example, if unauth_referrer_whitelist was
#set to :
#[ "example.com/myapp" ]
#then a request with a Referer header of
#http://example.com/myapp/login would be allowed.
unauth_referrer_whitelist = "unauth_referrer_whitelist"

#the locations property allows a way to specify more
#fine grained access controls.  if present, it must be
#a list of objects, and the first object found to be
#matching the request will be used to define the
#access controls for the request.  if no match is
#found, the default settings specified for the
#collection in general (i.e. outside the 'locations'
#property) will be used.
#
#each object in the locations list must have either a
#"prefix" or a "regexp" property.  this will be used
#to see if an incoming request is subject to the
#objects access controls.  each object in the list may
#then have zero or more of the above properties
#allowed for access control (i.e. allow_unauth_read,
#ipv4_whitelist, etc.) 
#
#In this way it is possible to have fine grained
#access controls over different parts of a collection.
#For example: 
#{   
#  "allow_unauth_read": false, 
#  "ipv4_whitelist": null, 
#  "prefix": "/abc"
#},  
#{   
#  "allow_unauth_read": false, 
#  "ipv4_whitelist": null, 
#  "prefix": "/def"
#}  
locations = "locations"

_current_version = "1.0"
_max_access_control_json_length = 16 * 1024

def _cleanse_version(entry):
    if type(entry) not in [str, unicode]:
        raise AccessControlCleanseError("invalid version type {0}".format(
            type(version)))
    version_str = entry.encode("utf-8")
    if version_str == _current_version:
        return version_str
    raise AccessControlCleanseError("unknown version {0}".format(version_str))

def _cleanse_bool(entry):
    if type(entry) in [bool, int]:
        if entry:
            return True
        return False
    raise AccessControlCleanseError(
        "Expected bool got {0}".format(type(entry)))

def _cleanse_ipv4_whitelist(entry):
    if entry is None:
        return None
    if type(entry) is not list:
        raise AccessControlCleanseError(
            "invalid ipv4_whitelist type {0}".format(type(entry)))

    ipv4_whitelist = list()
    for raw_element in entry:
        if type(raw_element) not in [str, unicode]:
            raise AccessControlCleanseError(
                "invalid ipv4_whitelist entry type {0}".format(
                    type(raw_element)))
       
        try:
            netblock = ipaddr.IPv4Network(raw_element)
        except ipaddr.AddressValueError, instance:
            raise AccessControlCleanseError(
                "invalid ipv4_whitelist address {0} {0}".format(
                    raw_element, instance))

        ipv4_whitelist.append(str(netblock))
        
    return ipv4_whitelist

def _cleanse_unauth_referrer_whitelist(entry):
    if entry is None:
        return None
    if type(entry) is not list:
        raise AccessControlCleanseError(
            "invalid unauth_referrer_whitelist type {0}".format(type(entry)))

    unauth_referrer_whitelist = list()
    for raw_element in entry:
        if type(raw_element) not in [str, unicode]:
            raise AccessControlCleanseError(
                "invalid unauth_referrer_whitelist entry type {0}".format(
                    type(raw_element)))
        # TODO: maybe we need to exclude some types of characters here
        unauth_referrer_whitelist.append(raw_element.encode("utf-8"))

    return unauth_referrer_whitelist

# we use _cleanse_dispatch_table in _cleanse_locations
# then we add _cleanse_locations to _cleanse_dispatch_table
# this is not recursive because 'locations' is not in 'valid_location_entries'

_cleanse_dispatch_table = {
    version                     : _cleanse_version,
    allow_unauth_read           : _cleanse_bool,
    allow_unauth_write          : _cleanse_bool,
    allow_unauth_list           : _cleanse_bool,
    allow_unauth_delete         : _cleanse_bool,
    ipv4_whitelist              : _cleanse_ipv4_whitelist,
    unauth_referrer_whitelist   : _cleanse_unauth_referrer_whitelist,
}

def _cleanse_locations(entry):
    valid_location_entries = set([allow_unauth_read,
                                  allow_unauth_write,
                                  allow_unauth_delete,
                                  ipv4_whitelist, 
                                  unauth_referrer_whitelist])
    if entry is None:
        return None
    if type(entry) is not list:
        raise AccessControlCleanseError(
            "invalid locations type {0}".format(type(entry)))

    locations = list()
    for raw_element in entry:
        if type(raw_element) is not dict:
            raise AccessControlCleanseError(
                "invalid locations entry type {0}".format(type(raw_element)))

        element = dict()
        # the keys in the raw element should be either 'prefix', 'regexp', or
        # a valid location entry
        for raw_key in raw_element.keys():
            if type(raw_key) not in [str, unicode]:
                raise AccessControlCleanseError(
                    "invalid locations entry key type {0}".format(type(raw_key)
                    ))
            key = raw_key.encode("utf-8")
            if key in ["prefix", "regexp", ]:
                raw_value = raw_element[key]
                if type(raw_value) not in [str, unicode]:
                    raise AccessControlCleanseError(
                        "invalid locations prefix type {0}".format(
                        type(raw_value)))

                # TODO: maybe we need to exclude some types of characters here
                value = raw_value.encode("utf-8")

                if key == "regexp":
                    try:
                        re.compile(value)
                    except Exception, instance:
                        raise AccessControlCleanseError(
                            "invalid locations regexp {0} {1}".format(
                            value, instance))

                element[key] = value
            elif key not in valid_location_entries:
                raise AccessControlCleanseError(
                    "invalid locations entry key {0}".format(key))
            else:
                raw_value = raw_element[key]
                element[key] = _cleanse_dispatch_table[key](raw_value)
        locations.append(element)

    return locations

_cleanse_dispatch_table[locations] = _cleanse_locations

def _normalize_path(path):
    """
    Put a url path in a form that we can compare
     * no leading slash
     * lower case
    """
    test_path = path.lower()
    while test_path.startswith("/"):
        test_path = test_path[1:]
    return test_path

def _apply_location_modifications(baseline_access_control, url):
    """
    to handle the 'location' modifications, we make a deep copy of
    the access_control dict and modify it. We assume that the dict
    is small enough to make this inexpensive
    """
    log = logging.getLogger("_apply_location_modifications")
    parsed_url = urlparse(url)
    path = _normalize_path(parsed_url.path)

    # see if our path matches any locations entries
    location_index = None
    location_discriminator = None
    for index, location in enumerate(baseline_access_control[locations]):
        if "prefix" in location:
            prefix = _normalize_path(location["prefix"])
            log.debug("matching {0} {1}".format(path, prefix))
            if path.startswith(prefix):
                log.debug("match {0}".format(location))
                location_index = index
                location_discriminator = "prefix"
                break
        elif "regexp" in location:
            regexp = re.compile(location["regexp"], flags=re.IGNORECASE)
            log.debug("matching {0} {1}".format(path, regexp))
            match_object = regexp.match(path)
            if match_object is not None:
                log.debug("match {0}".format(location))
                location_index = index
                location_discriminator = "regexp"
                break
        else:
            log.error("unparsable location entry {0}".format(location))

    # if we don't match any location entries, return the access_control 
    # unchanged
    if location_index is None:
        return baseline_access_control

    # create a deep copy of the baseline dict
    # and update it with the location specific entries
    access_control = deepcopy(baseline_access_control)

    location_entry = access_control[locations][location_index]

    # we probably don't have to restore the location entry,
    # but it doesn't cost much and it might save some confusion downstream
    discriminator_value = location_entry.pop(location_discriminator)
    access_control.update(location_entry)
    location_entry[location_discriminator] = discriminator_value

    return access_control

def _check_ipv4_whitelist(raw_whitelist, raw_remote_addr):
    """
    return True if the remote_addr is included in one of the networks
    defined in the whitelist.
    """
    whitelist = [ipaddr.IPv4Network(a) for a in raw_whitelist]
    remote_addr = ipaddr.IPv4Address(raw_remote_addr)

    for network in whitelist:
        if remote_addr in network:
            return True

    return False

def _check_unauth_referrer_whitelist(raw_whitelist, headers):
    """
    return True if the request has a 'Referer' header
    and one of the prefixes in the whitelist matches the first part of
    the path part of the URI 
    """
    # the webob headers dict is case insensitive
    if "Referer" not in headers:
        return False

    whitelist = [_normalize_path(p) for p in raw_whitelist]

    parsed_referrer = urlparse(headers["Referer"])
    path = _normalize_path(parsed_referrer.path)
    test_path = "/".join([parsed_referrer.netloc.lower(), path])

    for prefix in whitelist:
        if test_path.startswith(prefix):
            return True
            
    return False

def cleanse_access_control(raw_access_control):
    """
    raw_access_control
        raw text, presumably uploaded from a (possibly malicious) client

    returns a tuple (access_control, error_messages)
        if successful acccess_control will be a JSON string
                      error_messages will be None
        if unsucessful, access_control will be None and 
                        error_messages will be a list of strings

    Reject the input without parsing it if it is longer than some reasonable 
    max length (maybe 16k.)

    Load the user provided JSON into a Python variable via json.loads.

    Explicitly pick specific terms out of the Python variable into a second 
    Python variable. I.e. loop over the JSON explicitly looking for every term 
    that we care about. Reject the request at this stage if the JSON contains 
    terms we didn't look for, or if there are invalid values for terms, or 
    invalid combinations of terms.

    (The rejection should be a JSON response with success: false, and an 
    error_messages array containing one or more strings describing errors.)

    If all is well, save to our database JSON serialized from the 2nd Python 
    data structure.

    In other words, we save well formed JSON re-serialized from validated input
    """
    valid_dict = dict()
    error_message_list = list()

    if raw_access_control is None:
        return None, None

    if len(raw_access_control) > _max_access_control_json_length:
        error_message = \
            "JSON text too large {0} bytes".format(len(raw_access_control))
        error_message_list.append(error_message)
        return None, error_message_list

    try:
        raw_dict = json.loads(raw_access_control)
    except Exception, instance:
        error_message = \
            "Unable to parse access_control JSON {0}".format(instance)
        error_message_list.append(error_message)
        return None, error_message_list

    for key, cleanse_function in _cleanse_dispatch_table.items():
        try:
            raw_entry = raw_dict.pop(key)
        except KeyError:
            continue

        try:
            entry = cleanse_function(raw_entry)
        except AccessControlCleanseError, instance:
            error_message_list.append(str(instance))
        else:
            valid_dict[key] = entry

    if len(raw_dict) > 0:
        error_message = "unidentified keys in upload"
        error_message_list.append(error_message)

    if len(error_message_list) > 0:
        return None, error_message_list

    return json.dumps(valid_dict), None

def check_access_control(access_type, request, baseline_access_control_json):
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
    log = logging.getLogger("check_access_control")

    # if no special access control is specified, we must authenticate
    if baseline_access_control_json is None:
        log.debug("access_control is None")
        return access_requires_password_authentication

    try:
        baseline_access_control = json.loads(baseline_access_control_json)
    except Exception, instance:
        log.error("Unable to parse access_control JSON {0}".format(
            instance))
        return access_forbidden

    if len(baseline_access_control) == 0:
        log.debug("access_control is empty")
        return access_requires_password_authentication

    # if the access_control data is not a known version, something is wrong
    if not version in baseline_access_control or \
        baseline_access_control[version] != _current_version:
        log.error("invalid version {0}".format(baseline_access_control))
        return access_forbidden

    if locations in baseline_access_control:
        access_control = _apply_location_modifications(baseline_access_control,
                                                       request.url)
    else:
        access_control = baseline_access_control

    # ipv4_whitelist applies to ALL requests. 
    # Specifying an ipv4_whitelist means that ALL requests, 
    # unauthenticated or authenticated, 
    # must be specifically included in the white list, 
    # or they will be rejected.
    if ipv4_whitelist in access_control and \
        len(access_control[ipv4_whitelist]) > 0:
        included = _check_ipv4_whitelist(access_control[ipv4_whitelist],
                                         request.remote_addr)
        log.debug("included in ipv4_whitelist = {0}".format(included))
        if not included:
            return access_forbidden

    # The unauth_referrer_whitelist is a further restriction on which URLs 
    # an unauthenticated request claim to be originating from. 
    # It has no effect on authenticated requests. Just because a request meets 
    # the requirements of unauth_referrer_whitelist alone does not mean it is 
    # automatically allowed. It is allowed only if it should be allowed 
    # according to allow_unauth_read, allow_unauth_write, etc. 
    if unauth_referrer_whitelist in access_control and \
        len(unauth_referrer_whitelist) > 0:
        match = \
            _check_unauth_referrer_whitelist(
                access_control[unauth_referrer_whitelist],
                request.headers)
        log.debug("match in unauth_referrer_whitelist = {0}".format(match))
        if not match:
            return access_forbidden

    if access_type == read_access and \
        access_control.get(allow_unauth_read, False):
        log.debug("read access allowed due to allow_unauth_read")
        return access_allowed

    if access_type == write_access and \
        access_control.get(allow_unauth_write, False):
        log.debug("write access allowed due to allow_unauth_write")
        return access_allowed

    if access_type == list_access and \
        access_control.get(allow_unauth_list, False):
        log.debug("list access allowed due to allow_unauth_list")
        return access_allowed

    if access_type == delete_access and \
        access_control.get(allow_unauth_delete, False):
        log.debug("delete access allowed due to allow_unauth_delete")
        return access_allowed

    # if no access_control clause applies to this request, we must authenticate
    log.debug("no access control applies")
    return access_requires_password_authentication

