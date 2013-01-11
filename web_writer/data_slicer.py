# -*- coding: utf-8 -*-
"""
data_slicer.py

A generator that splits an uploaded body into slices.
"""
import logging

import gevent

from tools.data_definitions import incoming_slice_size

class SliceGeneratorTimeout(object):
    pass

# Ticket #69 Protection in Web Writer from Slow Uploads
_MAX_SEQUENCE_UPLOAD_INTERVAL = 300

def slice_generator(file_obj):
    log = logging.getLogger("slice_generator")
    buf = ''
    while True:
        timeout = gevent.Timeout(_MAX_SEQUENCE_UPLOAD_INTERVAL,
                                 SliceGeneratorTimeout)
        timeout.start()
        data = file_obj.read(incoming_slice_size)
        timeout.cancel()
        log.debug("read {0} bytes buf len = {1}".format(len(data),
                                                        len(buf)))

        if len(data) == 0:
            if len(buf) > 0:
                yield buf
            raise StopIteration()
        buf = "".join([buf, data, ])
        yield buf[:incoming_slice_size]
        buf = buf[incoming_slice_size:]

