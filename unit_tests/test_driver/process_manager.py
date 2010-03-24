"""
process_manager.py

Manage spawned processes
"""
import logging
import os
import os.path
import signal
import subprocess
import sys

class ProcessManager(object):
    """Manage spawned processes"""
    
    def __init__(self, process_count, exclude_list, python_files):
        self._log = logging.getLogger("ProcessManager")
        self._process_count = process_count
        self._exclude_list = exclude_list
        self._python_file_iter = iter(python_files)
        self._all_tests_started = False
        self._active_processes = set()
        self.error_count = 0
                
    def poll(self):
        """
        check on spawned procesess
        return the number of active processes
        when this goes to zero, all tests are done
        """
        expired_processes = list()
        for process in self._active_processes:
            returncode = process.poll()
            if returncode is None: # still running
                continue
            expired_processes.append(
                (process, returncode, )
            )
           
        for process, _ in expired_processes: 
            self._active_processes.remove(process)

        # 2008-10-14 dougfort -- start new processes before cleaning up the
        # old ones: it takes significant time
        if not self._all_tests_started:
            while len(self._active_processes) < self._process_count:
                
                try:
                    python_file = self._python_file_iter.next()
                except StopIteration: 
                    self._all_tests_started = True
                    break
                    
                if python_file in self._exclude_list:
                    continue
                
                process = self._start_new_process(python_file)
                self._active_processes.add(process)
            

        for process, returncode in expired_processes:
            process.wait()
            if returncode == 0:
                self._log.info(
                    "process '%s' terminated normally" % (process.pid,)
                )
                print process.stdout.read()
                print process.stderr.read()
            else:
                print process.stdout.read()
                self._log.info("process '%s' terminated by signal %s" % (
                    process.pid, -returncode, 
                ))
                self.error_count += 1
                stderr_result = "".join(process.stderr_content)
                print
                print '*' * 40
                print '*', process.file_name
                print '*' * 40
                print process.stderr.read()
                self._log.error(stderr_result)
            
        
        return len(self._active_processes)
        
    def close(self):
        """shut down spawned processed"""
        for process in self._active_processes:
            self._log.info("Sending SIGTERM to %s" % (process.pid, ))
            try:
                os.kill(process.pid, signal.SIGTERM)
            except:
                self._log.exception("Nonfatal: SIGTERM %s" % (process.pid, ))

    def _start_new_process(self, python_file):
                
        args = [sys.executable, python_file, ]
                    
        process = subprocess.Popen(
            args, 
            stdin=None, 
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        setattr(process, "stderr_content", list())
        setattr(process, "file_name", python_file)

        self._log.debug("Started process %s pid=%s" % (
            python_file, process.pid, 
        ))
        
        return process
        
        
