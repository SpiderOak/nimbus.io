#!/bin/bash
export SPIDEROAK_DIY="${HOME}/spideroak_diyapi"
export PYTHONPATH="${SPIDEROAK_DIY}"
export SPIDEROAK_MULTI_NODE_NAME="multi-node-01"
export DIYAPI_ANTI_ENTROPY_SERVER_ADDRESS="tcp://127.0.0.1:8600"

rm /var/log/pandora/diy_sim.log
python "${SPIDEROAK_DIY}/test/request_audit.py" "$1"
