import logging

def iter_exception_logger(log_name, prefix, iterator, *args, **kwargs):
    log = logging.getLogger(log_name)
    try:
        for step in iterator(*args, **kwargs): 
            yield step
    except Exception as instance:
        log.error(prefix + str(instance))
        log.exception(instance)
        raise