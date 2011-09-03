#!/bin/bash
source ${TOOL}/bin/env.sh
export NIMBUSIO="${HOME}/spideroak_diyapi"
export PYTHONPATH="${NIMBUSIO}"
export NIMBUSIO_LOG_DIR="/var/log/nimbusio"
export NIMBUSIO_REPOSITORY_PATH="/var/nimbusio"
export NIMBUSIO_CENTRAL_PASSWORD="0.332009188365191"
export NIMBUSIO_NODE_PASSWORD="0.383047103416175"

rm "${NIMBUSIO_LOG_DIR}/nimbusio_sim.log
python "${NIMBUSIO}/test/nimbusio_sim/nimbusio_sim_main.py"
