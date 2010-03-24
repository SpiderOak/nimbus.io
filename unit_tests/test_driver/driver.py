"""
driver.py

A driver program:

1. Identify all Python source files (*.py) in the unit_tests folder.
2. spawn N instances of unittest, running one for each source file.
3. Collate and report results

This program assumes that PYTHONPATH is set to the top of the source tree
(for example, 'trunk'). It uses PYTHONPATH to locate the unit_tests directory
unless overridden from the commandline.
"""
import glob
import logging
import os
import os.path
import signal
import sys
import time

from unit_tests.test_driver.process_manager import ProcessManager

class ConfigError(Exception):
    pass
    
class SigTermException(Exception):
    pass
    
def _parse_command_line():
    """Parse the command line, returning an options object"""
    from optparse import OptionParser

    parser = OptionParser()
    parser.add_option(
        '-f', "--log-file", dest="log_file", type="string",
        help="path of the log file"
    )
    parser.add_option(
        '-l', "--log-level", dest="log_level", type="string",
        help="set the root log level"
    )
    parser.add_option(
        '-c', "--process-count", dest="process_count", type="int",
        help="number of active processes to maintain"
    )
    parser.add_option(
        '-i', "--polling_interval", dest="polling_interval", type="float",
        help="number of seconds to run to wait between checks on proceses"
    )
    parser.add_option(
        '-u', "--unit-test-dir", dest="unit_test_dir", type="string",
        help="directory where unit test source files are"
    )
    parser.add_option(
        '-x', "--exclude", dest="exclude_list", type="string", action="append", 
        help="file to be excluded"
    )
    
    parser.set_defaults(log_file="test_driver.log")
    parser.set_defaults(log_level="debug")
    parser.set_defaults(process_count=1)
    parser.set_defaults(polling_interval=0.5)
    parser.set_defaults(unit_test_dir=None)
    parser.set_defaults(exclude_list=list())
    
    options, _ = parser.parse_args()
    
    options.log_level = options.log_level.upper()
    options.log_file = options.log_file
    if options.unit_test_dir is not None:
        options.unit_test_dir = options.unit_test_dir
    if "__init__.py" not in options.exclude_list:
        options.exclude_list.append("__init__.py")
    
    return options
    
def _initialize_logging(options):
    """initialize the log"""
    
    if not options.log_level in logging._levelNames:
        raise ConfigError("Unknown log level: %s" % options.log_level)
    log_level = logging._levelNames[options.log_level]      
    
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s %(levelname)-8s %(name)-20s: %(message)s',
        filename=options.log_file,
        filemode='w'
    )
    
def _signal_handler(signum, frame):
    raise SigTermException()
    
def _identify_source_path(options):
    if options.unit_test_dir is None:
        python_path = os.environ["PYTHONPATH"]
        for work_path in python_path.split(os.pathsep):
            source_path = os.path.join(work_path, "unit_tests")
            if os.path.isdir(source_path):
                break
    else:
        source_path = options.unit_test_dir
        
    return source_path
                
if __name__ == "__main__":
    options = _parse_command_line()
    _initialize_logging(options)
    log = logging.getLogger("main")
    log.info("Program starts: process_count=%s" % (
        options.process_count,
    ))

    source_path = _identify_source_path(options)
    os.chdir(source_path)
    python_files = glob.glob("*.py")
    log.info("using source path %s running %s python files" % (
        source_path, len(python_files), 
    ))
    
    process_manager = ProcessManager(
        options.process_count, options.exclude_list, python_files
    )
    
    signal.signal(signal.SIGTERM, _signal_handler)
    
    start_time = time.time()
    while True:
        current_time = time.time()
        elapsed_time = current_time - start_time
        try:
            active_processes = process_manager.poll()
            if active_processes == 0:
                log.info("all processes complete: %s errors" % (process_manager.error_count, ))
                break
            time.sleep(options.polling_interval)
        except KeyboardInterrupt:
            log.info("Caught KeyboardInterrupt: exiting")
            break
        except SigTermException:                
            log.info("Caught SigTermException: exiting")
            break
            
    log.info("Program ends")
    sys.exit(process_manager.error_count)
    
