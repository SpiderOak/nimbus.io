#!/bin/bash
export NIMBUSIO="${HOME}/nimbus.io"
export PYTHONPATH="${NIMBUSIO}"
export NIMBUSIO_LOG_DIR="/var/log/nimbusio"
export NIMBUSIO_NODE_NAME="multi-node-01"
export NIMBUSIO_EVENT_PUBLISHER_PUB_ADDRESS="tcp://127.0.0.1:8800"

rm "${NIMBUSIO_LOG_DIR}/event_subscriber.log"
python "${NIMBUSIO}/test/event_subscriber.py"
