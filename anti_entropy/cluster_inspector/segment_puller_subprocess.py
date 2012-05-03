# -*- coding: utf-8 -*-
"""
segment_puller_subprocess.py
  
a subprocess runby cluster_inspector to pull segment data from individual
node databases
"""
from collections import namedtuple
import gzip
import logging
import os
import os.path
import sys

from tools.standard_logging import initialize_logging 
from tools.database_connection import get_node_connection
from tools.data_definitions import segment_row_template
from tools.sized_pickle import store_sized_pickle

from anti_entropy.cluster_inspector.util import compute_segment_file_path, \
        compute_damaged_segment_file_path

_local_node_name = os.environ["NIMBUSIO_NODE_NAME"]
_node_names = os.environ["NIMBUSIO_NODE_NAME_SEQ"].split()
_node_database_hosts = \
    os.environ["NIMBUSIO_NODE_DATABASE_HOSTS"].split()
_node_database_ports = \
    os.environ["NIMBUSIO_NODE_DATABASE_PORTS"].split()
_node_database_passwords = \
    os.environ["NIMBUSIO_NODE_USER_PASSWORDS"].split() 
_damaged_segment_template = namedtuple("DamagedSegment", [
    "unified_id", "conjoined_part", "sequence_numbers"])
                                                  
def _pull_segment_data(connection, work_dir, node_name):
    """
    write out a dict for eqch segment_sequence
    """
    log = logging.getLogger("_pull_segment_data")
    result_generator = connection.generate_all_rows("""
        select {0} from nimbusio_node.segment
        where status <> 'C'
        order by unified_id, conjoined_part, handoff_node_id nulls last
    """.format(",".join(segment_row_template._fields), []))

    segment_row_count = 0

    segment_file_path = compute_segment_file_path(work_dir, node_name)
    segment_file = gzip.GzipFile(filename=segment_file_path, mode="wb")
    handoff_rows = list()
    for result in result_generator:
        segment_row = segment_row_template._make(result)
        if segment_row.file_hash is not None:
            segment_row = segment_row._replace(
                file_hash=bytes(segment_row.file_hash))
        if segment_row.handoff_node_id is not None:
            handoff_rows.append(segment_row._asdict())
            continue
        for handoff_row in handoff_rows:
            assert handoff_row["unified_id"] == segment_row.unified_id
            assert handoff_row["conjoined_part"] == segment_row.conjoined_part
        segment_dict = segment_row._asdict()
        segment_dict["handoff_rows"] = handoff_rows 
        store_sized_pickle(segment_dict, segment_file)
        segment_row_count += 1 
        handoff_rows = list()
    segment_file.close()

    log.info("stored {0} segment rows".format(segment_row_count))

def _pull_damaged_segment_data(connection, work_dir, node_name):
    """
    write out a tuple for each damaged segment_sequence
    """
    log = logging.getLogger("_pull_damaged_segment_data")
    result_generator = connection.generate_all_rows("""
        select unified_id, conjoined_part, sequence_numbers 
        from nimbusio_node.damaged_segment
        order by unified_id, conjoined_part""", [])
    damaged_segment_count = 0
    damaged_segment_file_path = \
            compute_damaged_segment_file_path(work_dir, node_name)
    damaged_segment_file = \
            gzip.GzipFile(filename=damaged_segment_file_path, mode="wb")

    unified_id = None
    conjoined_part = None
    sequence_numbers = list()
    for result in result_generator:
        damaged_segment_row = _damaged_segment_template._make(result)
        if unified_id is None:
            unified_id = damaged_segment_row.unified_id
            conjoined_part = damaged_segment_row.conjoined_part

        if damaged_segment_row.unified_id == unified_id and \
           damaged_segment_row.conjoined_part == conjoined_part:
            sequence_numbers.extend(damaged_segment_row.sequence_numbers)
            continue

        assert len(sequence_numbers) > 0
        damaged_segment_dict = {"unified_id"        : unified_id, 
                                "conjoined_part"    : conjoined_part, 
                                "sequence_numbers"  : sequence_numbers, }
        store_sized_pickle(damaged_segment_dict, damaged_segment_file)
        damaged_segment_count += 1

        unified_id = damaged_segment_row.unified_id
        conjoined_part = damaged_segment_row.conjoined_part
        sequence_numbers = damaged_segment_row.sequence_numbers

    log.info("stored {0} damaged segment entries".format(damaged_segment_count))

def main():
    """
    main entry point
    """
    [work_dir, index_str, ] = sys.argv[1:]
    index = int(index_str)
    node_name = _node_names[index]
    database_host = _node_database_hosts[index]
    database_port = _node_database_ports[index]
    database_password = _node_database_passwords[index]

    log_path = "{0}/nimbusio_segment_puller_from_{1}_to_{2}.log".format(
        os.environ["NIMBUSIO_LOG_DIR"], node_name, _local_node_name)
    initialize_logging(log_path)
    log = logging.getLogger("main")

    log.info("program starts: work_dir={0}, index={1}, {2}".format(
        work_dir, index, node_name))

    try:
        connection = get_node_connection(node_name,
                                         database_password,
                                         database_host,
                                         database_port)
    except Exception as instance:
        log.exception("Unable to connect to database {0}".format(instance))
        return -1

    try:
        _pull_segment_data(connection, work_dir, node_name)
        _pull_damaged_segment_data(connection, work_dir, node_name)
    except Exception as instance:
        log.exception("_pull_segment_data failed {0}".format(instance))
        return -2
    finally:
        connection.close()

    log.info("program terminates normally")
    return 0
    
if __name__ == "__main__":
    sys.exit(main())

