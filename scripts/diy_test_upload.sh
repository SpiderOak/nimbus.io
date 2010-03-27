#!/bin/bash
export TOOL="/opt/so2.6"
source ${TOOL}/bin/env.sh
export SPIDEROAK_DIY="${HOME}/spideroak_diyapi"
export PYTHONPATH="${SPIDEROAK_DIY}"

export SPIDEROAK_MULTI_NODE_NAME="node-sim-00"
export RABBITMQ_NODENAME="${SPIDEROAK_MULTI_NODE_NAME}"
export RABBITMQ_NODE_IP_ADDRESS="127.0.0.1"
export RABBITMQ_NODE_PORT="6000"

rm /var/log/pandora/diy_test_upload.log
${TOOL}/bin/python "${SPIDEROAK_DIY}/test/test_upload.py" "$1"
