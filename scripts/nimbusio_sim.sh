#!/bin/bash
export NIMBUSIO="${HOME}/nimbus.io"
export PYTHONPATH="${NIMBUSIO}"
export NIMBUSIO_LOG_DIR="/var/log/nimbusio"
export NIMBUSIO_CENTRAL_PASSWORD="0.332009188365191"
export NIMBUSIO_NODE_PASSWORD="0.383047103416175"

export TEST_BASE_PATH="/var/nimbusio"

rm "${NIMBUSIO_LOG_DIR}/nimbusio_sim.log"
python "${NIMBUSIO}/test/nimbusio_sim/nimbusio_sim_main.py"
