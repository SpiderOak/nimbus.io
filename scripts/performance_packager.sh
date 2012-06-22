#!/bin/bash
export NIMBUSIO="${HOME}/git/nimbus.io"
export PYTHONPATH="${NIMBUSIO}"
export NIMBUSIO_LOG_DIR="/var/log/nimbusio"
export NIMBUSIO_EVENT_AGGREGATOR_PUB_ADDRESS="tcp://127.0.0.1:8900"
export NIMBUSIO_NODE_NAME="multi-node-01"
PYTHON="python2.7"

"${PYTHON}" "${NIMBUSIO}/performance_packager/performance_packager_main.py"

