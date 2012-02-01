# -*- coding: utf-8 -*-
"""
nimbusio_customer.py

nimbus.io customer functions

 1. create a customer
 2. add a key to a customer
 3. list the details of a customer
"""
import sys

from tools.database_connection import get_central_connection
from tools.customer import create_customer, \
   add_key_to_customer, \
   list_customer_keys, \
   purge_customer

_create_customer = "create_customer"
_add_key = "add_key"
_list_keys = "list_keys"
_delete_customer = "delete_customer"
_purge_customer = "purge_customer"

def _parse_command_line():
    """Parse the command line, returning an options object"""
    from optparse import OptionParser

    parser = OptionParser()
    parser.add_option(
        '-u', "--username", dest="username", type="string",
        help="username of the customer"
    )
    parser.add_option(
        '-c', "--create-customer", dest="command", action="store_const", 
        const=_create_customer, 
        help="create a new customer, add one key, write values to stdout"
    )
    parser.add_option(
        '-a', "--add-key", dest="command", action="store_const",
        const=_add_key, help="add a key to an existing customer"
    )
    parser.add_option(
        '-l', "--list-keys", dest="command", action="store_const",
        const=_list_keys, help="list the customer's keys"
    )
    parser.add_option(
        '-d', "--delete-customer", dest="command", action="store_const",
        const=_delete_customer, help="mark a customer as deleted"
    )
    parser.add_option(
        '-p', "--purge-customer", dest="command", action="store_const",
        const=_purge_customer, help="remove a customer from the database"
    )
    parser.add_option(
        "--versioning", dest="versioning", action="store_true",
        help="does user have versioning on his default collection"
    )
    parser.set_defaults(versioning=False)

    options, _ = parser.parse_args()

    if options.username is None:
        print >> sys.stderr, "You must enter a user name"
        sys.exit(1)

    if options.command is None:
        print >> sys.stderr, "You must specify exactly one command"
        sys.exit(1)

    return options

def _handle_create_customer(connection, options):
    create_customer(connection, options.username, options.versioning)
    add_key_to_customer(connection, options.username)
    key_id, key = list_customer_keys(connection, options.username)[0]
    print "Username",  options.username
    print "AuthKeyId",  str(key_id)
    print "AuthKey", key
    print

def _handle_add_key(connection, options):
    print >> sys.stderr, "adding key to customer", options.username
    add_key_to_customer(connection, options.username)
    print >> sys.stderr, "key added"

def _handle_list_keys(connection, options):
    print >> sys.stderr, "listing customer keys", options.username
    for key_id, key in list_customer_keys(connection, options.username):
        print >> sys.stderr, "%6d" % key_id, key
    print >> sys.stderr, "listed"

def _handle_delete_customer(_connection, options):
    print >> sys.stderr, "deleting customer", options.username
    print >> sys.stderr, "*** not implemented ***"

def _handle_purge_customer(connection, options):
    print >> sys.stderr, "purging customer", options.username
    purge_customer(connection, options.username)
    print >> sys.stderr, "purged", options.username

_dispatch_table = {
    _create_customer    : _handle_create_customer,
    _add_key            : _handle_add_key,
    _list_keys          : _handle_list_keys,
    _delete_customer    : _handle_delete_customer,
    _purge_customer     : _handle_purge_customer,
}

def main():
    """
    main entry point
    """
    options = _parse_command_line()
    connection = get_central_connection()

    try:
        _dispatch_table[options.command](connection, options)
    except Exception:
        connection.rollback()
        raise
    else:
        connection.commit()
    finally:
        connection.close()

    return 0

if __name__ == "__main__":
    sys.exit(main())
