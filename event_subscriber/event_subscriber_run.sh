#!/bin/bash

PROG_DIR="$(cd $(dirname $0) ; pwd)"
export NIMBUSIO_LOG_LEVEL="DEBUG"

exec $PYTHON2 "${PROG_DIR}/event_subscriber_main.py" warn

