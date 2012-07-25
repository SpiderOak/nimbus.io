#!/bin/bash

PROG_DIR="$(cd $(dirname $0) ; pwd)"
PYTHON="python2.7"
export NIMBUSIO_LOG_LEVEL="DEBUG"
export PYTHONPATH="/home/dougfort/git/nimbus.io"

exec ${PYTHON} "${PROG_DIR}/web_monitor_main.py"

