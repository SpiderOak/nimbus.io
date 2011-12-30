#!/bin/bash
export NIMBUSIO="${HOME}/nimbus.io"
export PYTHONPATH="${NIMBUSIO}"
export NIMBUSIO_LOG_DIR="/var/log/nimbusio"
export NIMBUSIO_EVENT_AGGREGATOR_PUB_ADDRESS="tcp://127.0.0.1:8900"

rm "${NIMBUSIO_LOG_DIR}/event_subscriber.log"
python "${NIMBUSIO}/test/event_subscriber.py" "$1"
