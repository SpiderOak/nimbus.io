# -*- coding: utf-8 -*-
"""
options.py

get constant values from the environment, allowing override from the
command line
"""
import argparse
import logging
import os

# default constants; can be overridden from command line
_max_node_offline_time = os.environ.get("NIMBUSIO_GC_MAX_NODE_OFFLINE_TIME",  
                                        "30 days")
_max_sort_mem = int(
    os.environ.get("NIMBISIO_GC_MAX_SORT_MEM_GB", "3")
)
_max_value_file_size_to_agg = int(
    os.environ.get("NIMBISIO_GC_MAX_VALUE_FILE_SIZE_TO_AGG_MB", "10")
)
_min_savings_size = int(
    os.environ.get("NMBUSIO_GC_MIN_SAVINGS_SIZE_MB", "10")
)
_min_savings_ratio = float(
    os.environ.get("NIMBUSIO_GC_MIN_SAVINGS_RATIO", "0.03")
)

def _parse_command_line():
    """
    allow the user to overide constants from the command line
    """
    parser = argparse.ArgumentParser(description='Garbage Collection')
    parser.add_argument("-o", "--max-node-offline-time", 
                         type=str,
                         default=_max_node_offline_time,
                         help="tombstone garbage collection time in the format"
                              " of a postgresql interval")
    parser.add_argument("-m", "--max-sort-mem", 
                         type=int,
                         default=_max_sort_mem)
    parser.add_argument("-f", "--max-value-file-size-to-agg", 
                         type=int,
                         default=_max_value_file_size_to_agg)
    parser.add_argument("-s", "--min-savings-size", 
                         type=int,
                         default=_min_savings_size)
    parser.add_argument("-r", "--min-savings-ratio", 
                         type=float,
                         default=_min_savings_ratio)
    parser.add_argument("-c", "--collection-id", 
                         type=int,
                         default=None)
    return parser.parse_args()
        
def get_options():
    """
    get constants from the environment, allowing override by the 
    command line
    """
    log = logging.getLogger("get_optons")

    options = _parse_command_line()
    log.info("max_node_offline_time = {0}".format(
        options.max_node_offline_time
    ))
    log.info("max_sort_mem (gb) = {0}".format(options.max_sort_mem))
    log.info("max_value_file_size_to_agg (mb) = {0}".format(
        options.max_value_file_size_to_agg))
    log.info("min_savings_size (mb) = {0}".format(options.min_savings_size))
    log.info("min_savings_ratio = {0}".format( options.min_savings_ratio))
    if options.collection_id is not None:
        log.info("collection = ${0}".format(options.collection_id))

    return options

