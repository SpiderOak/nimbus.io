# -*- coding: utf-8 -*-
"""
process_conjoined_rows.py

process handoffs of conjoined rows
"""
import itertools
import logging

def _key_function(conjoined_row):
    return conjoined_row[1]["unified_id"]

def _generate_conjoined_rows(raw_conjoined_rows):
    """
    yield tuples of (source_node_ids, conjoined_row)

    where source_node_names is a list of the nodes where the conjoined rows 
    came from.

    We want to distill down all the conjoined_rows for a specific unified 
    id into a single final state based on the various timestamps.

     * If abort_timestamp is not None - return that row
     * If delete_timestamp is not None - return that row
     * If complete_timestamp is not None - return that row unless 
       one of the above is true
     * If none of the above is true, return any row
    """
    # sort on unified id to bring pairs together
    raw_conjoined_rows.sort(key=_key_function)

    for (_unified_id, ), group in itertools.groupby(raw_conjoined_rows, 
                                                    _key_function):
        conjoined_row_list = list(group)
        assert len(conjoined_row_list) > 0
        return_row = None
        source_node_names = set()
        for source_node_name, conjoined_row in conjoined_row_list:
            source_node_names.add(source_node_name)
            if conjoined_row["abort_timestamp"] is not None:
                return_row = conjoined_row
                continue
            if conjoined_row["delete_timestamp"] is not None:
                return_row = conjoined_row
                continue
            if conjoined_row["complete_timestamp"] is not None and \
                return_row is None:
                return_row = conjoined_row
                continue
        # if we get to this point and don't have a return_row,
        # we assume a conjoined archive is in progress and hand back
        # any row
        if return_row is None:
            return_row = conjoined_row_list[0][1]

        yield (list(source_node_names), return_row, )

def _process_conjoined_row(dest_database, conjoined_row):
    """
    the destination database may have a conjoined row that is
    newer than our row. That should only happen if the dest node
    got a completion or a delete that we don't have in the handoffs
    """
    cursor = dest_database.cursor()

    delete_query = """delete from nimbusio_node.conjoined
                      where unified_id = %(unified_id)s"""

    count_query = """select count(*) from nimbusio_node.conjoined
                     where unified_id = %(unified_id)s
                     and (   abort_timestamp is not null 
                          or complete_timestamp is not null
                          or delete_timestamp is not null)"""

    insert_query = """insert into nimbusio_node.conjoined (
                      collection_id, 
                      key, 
                      unified_id, 
                      create_timestamp, 
                      abort_timestamp,
                      complete_timestamp,
                      delete_timestamp,
                      combined_size)
                      values (
                      %(collection_id)s, 
                      %(key)s, 
                      %(unified_id)s, 
                      %(create_timestamp)s::timestamp,
                      %(abort_timestamp)s::timestamp,
                      %(complete_timestamp)s::timestamp,
                      %(delete_timestamp)s::timestamp,
                      $(combined_size)s)"""         

    replace_destination = False
    # if we have an abort or a delete, we don't care what is at the destination
    # we go ahead and replace any existing row
    if conjoined_row["abort_timestamp"] is not None or \
        conjoined_row["delete_timestamp"] is not None:
        replace_destination = True
    # otherwise, if there is a row at the  destination and it has 
    # an abort, a delete or a completion
    # we assume it is up-to-date and do not replace it
    else:
        cursor.execute(count_query, conjoined_row)
        (count, ) = cursor.fetchone()
        replace_destination = (count == 0)

    if replace_destination:
        cursor.execute("begin")
        cursor.execute(delete_query, conjoined_row)
        cursor.execute(insert_query, conjoined_row)
            
    cursor.close()
    if replace_destination:
        dest_database.commit()

def _purge_conjoined_handoffs(node_databases, 
                              source_node_names,  
                              unified_id,
                              handoff_node_id):
    """
    remove handoff conjoined from local database(s)
    """
    log = logging.getLogger("_purge_conjoined_handoffs")
    query = """
        delete from nimbusio_node.conjoined
        where unified_id = %(unified_id)s
        and handoff_node_id = %(handoff_node_id)s;
        """
    arguments = {"unified_id" : unified_id, 
                 "handoff_node_id" : handoff_node_id}

    for source_node_name in source_node_names:
        cursor = node_databases[source_node_name].cursor()
        cursor.execute(query, arguments)
        if cursor.rowcount != 1:
            log.error("purged {0} rows {1}".format(cursor.rowcount,
                                                   arguments))
        cursor.close()
        node_databases[source_node_name].commit()

def process_conjoined_rows(halt_event, 
                           args,
                           node_databases, 
                           source_node_names, 
                           raw_conjoined_rows):
    """
    process handoffs of conjoined rows
    """
    log = logging.getLogger("process_conjoined_rows")

    dest_database = node_databases[args.node_name]

    # loop until all handoffs have been accomplished
    log.debug("start handoffs")
    work_generator =  _generate_conjoined_rows(raw_conjoined_rows)
    while not halt_event.is_set():

        # get a conjoined row to process. break at EOF
        try:
            source_node_names, conjoined_row = next(work_generator)
        except StopIteration:
            break

        log.info("handing off {0} to {1}".format(
                                  conjoined_row["unified_id"],
                                  conjoined_row["handoff_node_id"]))

        _process_conjoined_row(dest_database, conjoined_row)
        _purge_conjoined_handoffs(node_databases, 
                                  source_node_names,
                                  conjoined_row["unified_id"],
                                  conjoined_row["handoff_node_id"])

