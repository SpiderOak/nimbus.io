#!/bin/bash

PROG_DIR="$(cd $(dirname $0) ; pwd)"

export NIMBUSIO_LOG_LEVEL="DEBUG"

GUNICORN=$(dirname $(readlink -f $(which ${PYTHON2:?})))/gunicorn

cd $PROG_DIR

exec $GUNICORN \
    -b unix:${NIMBUSIO_SOCKET_DIR:?}/${NIMBUS_IO_RUN_PROGRAM_NAME:?}.sock \
    -w ${NIMBUSIO_WEBSERVICE_NUM_WORKERS:-4} \
    ${NIMBUS_IO_RUN_PROGRAM_NAME:?}_main:app
