#!/bin/bash
TEST_DIR="$1"
export NIMBUSIO="${HOME}/git/nimbus.io"
export PYTHONPATH="${NIMBUSIO}"
export NIMBUSIO_LOG_DIR="${TEST_DIR}/logs"
export NIMBUSIO_EVENT_AGGREGATOR_PUB_ADDRESS="tcp://127.0.0.1:8900"
PYTHON="python2.7"

rm "${NIMBUSIO_LOG_DIR}/event_subscriber.log"
"${PYTHON}" "${NIMBUSIO}/test/event_subscriber.py" "${TEST_DIR}"
