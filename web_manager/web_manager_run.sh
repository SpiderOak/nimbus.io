#!/bin/bash

PROG_DIR="$(cd $(dirname $0) ; pwd)"
PYTHON="python2.7"

exec ${PYTHON} "${PROG_DIR}/web_manager_main.py" \ 
   2>&1 > /var/log/pandora/nimbus/web_manager.log

