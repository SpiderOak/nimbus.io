#!/bin/bash

PROG_DIR="$(cd $(dirname $0) ; pwd)"

tproxy -b ${NIMBUS_IO_SERVICE_DOMAIN:?}:${NIMBUSIO_WEB_SERVER_PORT:?} \
    $PROG_DIR/web_director_main.py
