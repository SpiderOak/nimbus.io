#!/bin/bash

PROG_DIR="$(cd $(dirname $0) ; pwd)"
export NIMBUSIO_LOG_LEVEL="DEBUG"

exec $PYTHON2 "${PROG_DIR}/web_monitor_main.py" \
  "/etc/default/nimbus.io.web_monitor.${HOSTNAME}.json" 

