# -*- coding: utf-8 -*-
"""
nimbusio_customer.py

nimbus.io customer functions

 1. create a customer
 2. add a key to a customer
 3. list the details of a customer
"""
import sys

from diyapi_tools.database_connection import get_central_connection
from diyapi_tools.customer import create_customer, \
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
        const=_create_customer, help="create a new customer with his first key"
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

    options, _ = parser.parse_args()

    if options.username is None:
        print >> sys.stderr, "You must enter a user name"
        sys.exit(1)

    if options.command is None:
        print >> sys.stderr, "You must specify exactly one command"
        sys.exit(1)

    return options

def _handle_create_customer(connection, username):
    print >> sys.stderr, "creating customer", username
    create_customer(connection, username)
    print >> sys.stderr, "created"

def _handle_add_key(connection, username):
    print >> sys.stderr, "adding key to customer", username
    add_key_to_customer(connection, username)
    print >> sys.stderr, "key added"

def _handle_list_keys(connection, username):
    print >> sys.stderr, "listing customer keys", username
    for key_id, key in list_customer_keys(connection, username):
        print >> sys.stderr, "%6d" % key_id, key
    print >> sys.stderr, "listed"

def _handle_delete_customer(connection, username):
    print >> sys.stderr, "deleting customer", username
    print >> sys.stderr, "*** not implemented ***"

def _handle_purge_customer(connection, username):
    print >> sys.stderr, "purging customer", username
    purge_customer(connection, username)
    print >> sys.stderr, "purged", username

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
        _dispatch_table[options.command](connection, options.username)
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
