#!/bin/bash
export NIMBUSIO="${HOME}/nimbus.io"
export PYTHONPATH="${NIMBUSIO}"
export NIMBUSIO_LOG_DIR="/var/log/nimbusio"
export NIMBUSIO_NODE_NAME="multi-node-01"
export NIMBUSIO_ANTI_ENTROPY_SERVER_ADDRESS="tcp://127.0.0.1:8600"
export NIMBUSIO_CENTRAL_USER_PASSWORD="pork"

rm "${NIMBUSIO_LOG_DIR}/nimbusio_request_audit.log"
python "${NIMBUSIO}/test/request_audit.py" "$1"
