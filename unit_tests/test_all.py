# -*- coding: utf-8 -*-
"""
test_all.py

run all unit tests
"""
import os
import re
import unittest

_test_re = re.compile("test_.+?\.py$", re.IGNORECASE)
_filename_to_module = lambda f: os.path.splitext(f)[0]
_load = unittest.defaultTestLoader.loadTestsFromModule  

def _find_in_python_path(path_segments):
    """
    Find the path to a given item somewhere in PYTHONPATH
    """
    python_path = os.environ["PYTHONPATH"]
    for work_path in python_path.split(os.pathsep):
        test_path = os.path.join(work_path, path_segments)
        if os.path.isdir(unicode(test_path)):
            return test_path

    raise RuntimeError("Can't find %r in PYTHONPATH" % path_segments)

def _regression_test():
    """
    find all unit tests in a directory.
    This is adapted from Mark Pilgrim's 'Dive Into Python'
    """    
    path = _find_in_python_path("unit_tests")
    files = os.listdir(path)                               
    files = filter(_test_re.search, files)                     
    module_names = map(_filename_to_module, files)         
    modules = map(__import__, module_names)                 
    return unittest.TestSuite(map(_load, modules)) 

if __name__ == "__main__":
    test_suite = _regression_test()
    unittest.TextTestRunner(verbosity=2).run(test_suite)
