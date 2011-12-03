# -*- coding: utf-8 -*-
"""
Prepare .PNG grpaphs using gprof2dot and dot
"""
import logging
import os
import os.path
import subprocess
import sys

from test.nimbusio_sim.process_util import identify_program_dir

class ProfileGraphError(Exception):
    pass

_buffer_size = 1024 * 1024 * 8   # buffer size to use between subprocesses

def prepare_profile_graphs(profile_dir):
    log = logging.getLogger("prepare_profile_graphs")
    tools_dir = identify_program_dir("tools")
    if tools_dir is None:
        raise ProfileGraphError("unable to locate tools directory")
    gprof2dot_path = os.path.join(tools_dir, "gprof2dot.py")
    if not os.path.isfile(gprof2dot_path):
        raise ProfileGraphError("unable to locate gprof2dot.py")

    for file_name in os.listdir(profile_dir):
        if not file_name.endswith(".pstats"):
            continue
        log.debug(file_name)
        pstats_path = os.path.join(profile_dir, file_name)
        png_path = "".join([pstats_path[:-len("pstats")], "png"])

        gprof2dot_args = [
            sys.executable,
            gprof2dot_path,
            "-f",
            "pstats",
            "-e",
            "0.1",
            "-n",
            "0.1",
            pstats_path
        ]

        dot_args = [
            "usr/bin/dot",
            "-T",
            "png",
            "-o",
            png_path
        ]

        gprof2dot_process = subprocess.Popen(
            gprof2dot_args, stdout=subprocess.PIPE, bufsize=_buffer_size
        )
        dot_process = subprocess.Popen(
            dot_args, 
            stdin=gprof2dot_process.stdout, 
            stdout=subprocess.PIPE,
            bufsize=_buffer_size
        )
        dot_process.communicate()

        if gprof2dot_process.returncode != 0:
            if gprof2dot_process.returncode is None:
                gprof2dot_process.terminate()
            else:
                message = "gprof2dot process failed %s %s" % (
                    gprof2dot_process.returncode, gprof2dot_process.stderr,
                )
                log.error(message)
                raise ProfileGraphError(message)

        if dot_process.returncode != 0:
            message = "dot process failed %s %s" % (
                dot_process.returncode, dot_process.stderr,
            )
            log.error(message)
            raise ProfileGraphError(message) 

