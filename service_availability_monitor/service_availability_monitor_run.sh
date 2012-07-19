#!/bin/bash

PROG_DIR="$(cd $(dirname $0) ; pwd)"
PYTHON="python3.2"
export NIMBUSIO_LOG_LEVEL="DEBUG"

exec ${PYTHON} "${PROG_DIR}/service_availability_monitor_main.py"

