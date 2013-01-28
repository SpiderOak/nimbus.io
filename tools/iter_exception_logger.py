import logging

def iter_exception_logger(log_name, prefix, iterator, *args, **kwargs):
    log = logging.getLogger(log_name)
    try:
        for step in iterator(*args, **kwargs):
            log.debug("{0}: yielding {1} bytes".format(prefix,
                                                       len(step))) 
            yield step
    except Exception as instance:
        log.error(prefix + str(instance))
        log.exception(instance)
        raise