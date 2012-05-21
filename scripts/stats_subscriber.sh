#!/bin/bash
export NIMBUSIO="${HOME}/git/nimbus.io"
export PYTHONPATH="${NIMBUSIO}"
export NIMBUSIO_LOG_DIR="/var/log/nimbusio"
export NIMBUSIO_EVENT_AGGREGATOR_PUB_ADDRESS="tcp://127.0.0.1:8900"
PYTHON="python2.7"

rm "${NIMBUSIO_LOG_DIR}/stats_subscriber.log"
"${PYTHON}" "${NIMBUSIO}/test/stats_subscriber.py"
