#!/bin/bash

PROG_DIR="$(cd $(dirname $0) ; pwd)"
PYTHON="python2.7"
export NIMBUSIO_LOG_LEVEL="DEBUG"

exec ${PYTHON} "${PROG_DIR}/web_collection_manager_main.py"

