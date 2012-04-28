# -*- coding: utf-8 -*-
"""
node_data_reader.py

manage 10 subprocesses to read data from nodes
"""
import errno
import logging
import os 
import subprocess
import sys

from tools.sized_pickle import retrieve_sized_pickle

class NodeDataReaderError(Exception):
    pass

class NodeSubProcess(object):
    def __init__(self, node_name, process):
        self._log = logging.getLogger("NodeSubProcess({0})".format(node_name))
        self._node_name = node_name
        self._process = process
        self._entry = None
        self._eof = False

    def __getattr__(self, name):
        return self._entry[name]

    def advance_to_sequence_num(self, current_record_number, sequence_num):
        self._log.debug("advance_to_sequence_num {0}".format(sequence_num))
        if self._eof:
            return False

        assert  self._entry is not None
        if self._entry["record_number"] != current_record_number:
            return False

        if self._entry["sequence_num"] is None or \
           self._entry["sequence_num"] == sequence_num:
            return True

        assert self._entry["sequence_num"] == sequence_num - 1, \
            (self._entry["sequence_num"], sequence_num - 1, )

        if not self._get_next_entry():
            return False

        if self._entry["record_number"] != current_record_number:
            return False
        
        assert self._entry["sequence_num"] == sequence_num, \
            (self._entry["sequence_num"], sequence_num, )
        return True

    def advance_to_record_number(self, record_number):
        self._log.debug("advance_to_record_number {0}".format(record_number))
        if self._eof:
            return False

        if self._entry is not None:
            if self._entry["record_number"] == record_number:
                return True
            # XXX review: I'm glad to see these aserts. They are hints that
            # make reviewing and understanding the code much easier by
            # explicitly stating the expected state.
            
            # In this case, this assert implies that advance_to_record_number
            # will ALWAYS be called once per record number. I.e. it will never
            # be called with the intent of advancing several records to reach
            # the desired record number.  I suppose that's how it's called
            # anyway, so it probably doesn't matter.  But the name
            # "advance_to_record_number" sort of implies it might be called to
            # advance an arbitrary number of records forward in the stream.
            assert self._entry["record_number"] == record_number - 1

        # XXX review: how does this get us to the next record number, if there
        # are perhaps several sequences still to be yielded for the current
        # record number?  Don't we need to loop or recurse until we get the
        # first entry for the desired record number?  
        
        # maybe this implies that we will always be called in a certain order.
        # if so, we should at least add some assertions to enforce that.

        return self._get_next_entry()

    def close(self):
        try:
            self._process.terminate()
            self._process.wait()
        except OSError as instance:
            if instance.errno == errno.ESRCH:
                pass
            raise

    def _get_next_entry(self):
        try:
            self._entry = retrieve_sized_pickle(self._process.stdout)
        except EOFError:
            self._process.wait()
            self._process.poll()

            if self._process.returncode != 0:
                error_message = "subprocess {0} failed {1}".format(
                    self._node_name, self._process.returncode)
                self._log.error(error_message)
                raise NodeDataReaderError(error_message)

            self._log.info("EOF")
            self._eof = True
            self._entry = None
            return False

        return True

_node_names = os.environ["NIMBUSIO_NODE_NAME_SEQ"].split()
_read_buffer_size = int(
    os.environ.get("NIMBUSIO_ANTI_ENTROPY_READ_BUFFER_SIZE", 
                   str(10 * 1024 ** 2)))

_environment_list = ["PYTHONPATH",
                    "NIMBUSIO_LOG_DIR",
                    "NIMBUSIO_LOG_LEVEL",
                    "NIMBUSIO_NODE_NAME", 
                    "NIMBUSIO_NODE_NAME_SEQ", 
                    "NIMBUSIO_DATA_READER_ANTI_ENTROPY_ADDRESSES",
                    "NIMBUSIO_REPOSITORY_PATH", ]

from anti_entropy.anti_entropy_util import identify_program_dir

def _advance_to_record_number(node_subprocesses, current_record_number):

    # XXX review: using all these advance_* functions is so complicated.  It's
    # very hard to convince myself there aren't subtle errors in them.

    # why can't we just have all the streams output results in a loosly
    # consistent format (tuples with at least the first few terms consistent as
    # needed for sorting) and then use heapq.merge to iterate over all of them
    # in sorted order and yield batches?  We could probably eliminate much of
    # this detailed logic.  

    # If there's a good reason we need our own logic for this, I'll review all
    # these functions in detail. For now I'm just reading quickly.

    log = logging.getLogger("_advance_to_record_number")
    log.debug("advancing to {0}".format(current_record_number))
    current_sequence_num = None
    advanced_count = 0

    for node_subprocess in node_subprocesses:
        advanced = \
            node_subprocess.advance_to_record_number(current_record_number)
        if advanced:
            advanced_count += 1
            assert node_subprocess.record_number == current_record_number, \
                ( node_subprocess.record_number, current_record_number, )

            if current_sequence_num is None:
                if node_subprocess.sequence_num is not None:
                    current_sequence_num = node_subprocess.sequence_num

            assert node_subprocess.sequence_num is None or \
                node_subprocess.sequence_num == current_sequence_num, \
                    (node_subprocess.sequence_num, current_sequence_num, )

    if advanced_count == 0:
        return None

    return current_sequence_num

def _advance_to_sequence_num(node_subprocesses, 
                             current_record_number, 
                             current_sequence_num):
    log = logging.getLogger("_advance_to_sequence_num")
    log.debug("advancing to {0}".format(current_sequence_num))
    advanced_count = 0
    for node_subprocess in node_subprocesses:
        advanced = \
            node_subprocess.advance_to_sequence_num(current_record_number, 
                                                    current_sequence_num)
        if advanced:
            if node_subprocess.sequence_num is not None:
                advanced_count += 1

                assert node_subprocess.sequence_num == current_sequence_num, \
                    (node_subprocess.sequence_num, current_sequence_num, )

    return advanced_count > 0

def generate_node_data(halt_event):
    """
    collate data from subprocesses pulling from nodes
    """
    log = logging.getLogger("generate_node_data")
    node_subprocesses = _start_subprocesses(halt_event)

    # advance all subprocesses to prime the pump
    current_record_number = 1 
    current_sequence_num = \
        _advance_to_record_number(node_subprocesses, current_record_number)

    # XXX review: as above, it seems much of this logic could be substituted
    # for heapq.merge. No "advance" functions anywhere. then it's just this:

    #    undefined = object() # disambiguate from None
    #    batch, last_record_num, last_sequence_num = \
    #        list(), undefined, undefined
    #
    #    for record in heapq.merge(*node_reader_generators):
    #
    #        batch.append(record)
    #
    #        if last_record_num is undefined:
    #            last_record_num = record.record_number
    #        if last_sequence_num is undefined:
    #            last_sequence_num = record.sequence_num
    #
    #        if (
    #            record.record_number != last_record_num or
    #            record.sequence_num != last_sequence_num
    #        ):
    #            yield batch
    #            batch, last_record_num, last_sequence_num = \
    #                list(), undefined, undefined

    # then the main process gets one batch of data per segment and sequence_num
    

    while current_sequence_num is not None and not halt_event.is_set():
        
        result = {"record-number"   : current_record_number,
                  "status"          : list(),	 
                  "unified-id"      : node_subprocesses[0].unified_id,	 
                  "conjoined-part"  : node_subprocesses[0].conjoined_part,
                  "segment-num"     : list(),
                  "sequence-num"    : current_sequence_num,
                  "data"            : list(),}

        for node_subprocess in node_subprocesses:
            result["status"].append(node_subprocess.status)
            result["segment-num"].append(node_subprocess.segment_num)
            result["data"].append(node_subprocess.data)

        log.debug("{0} {1} {2} {3} {4} {5}".format(
            result["record-number"],
            result["status"],	 
            result["unified-id"],
            result["conjoined-part"],
            result["segment-num"],
            result["sequence-num"]))

        yield result

        if current_sequence_num is None or current_sequence_num == 0:
            current_sequence_num = None
        else:
            current_sequence_num += 1
            advanced = \
                _advance_to_sequence_num(node_subprocesses, 
                                         current_record_number,
                                         current_sequence_num)
            if not advanced:
                current_sequence_num = None

        if current_sequence_num is None:
            current_record_number += 1
            current_sequence_num = \
                _advance_to_record_number(node_subprocesses, 
                                          current_record_number)

    for node_subprocess in node_subprocesses:
        node_subprocess.close()

def _start_subprocesses(halt_event):
    """
    start subprocesses
    """
    log = logging.getLogger("start_subprocesses")
    subprocesses = list()

    environment = dict(
        [(key, os.environ[key], ) for key in _environment_list])

    anti_entropy_dir = identify_program_dir("anti_entropy")
    subprocess_path = os.path.join(anti_entropy_dir,
                               "cluster_repair",
                               "node_data_reader_subprocess.py")

    for index, node_name in enumerate(_node_names):

        if halt_event.is_set():
            log.info("halt_event set: exiting")
            return subprocesses

        log.info("starting subprocess {0}".format(node_name))
        args = [sys.executable, subprocess_path, str(index) ]
        process = subprocess.Popen(args, 
                                   bufsize=_read_buffer_size,
                                   stdout=subprocess.PIPE, 
                                   env=environment)
        assert process is not None
        subprocesses.append(NodeSubProcess(node_name, process))

    return subprocesses

