#!/bin/bash
export TOOL="/opt/so2.6"
source ${TOOL}/bin/env.sh
export SPIDEROAK_DIY="${HOME}/spideroak_diyapi"
export PYTHONPATH="${SPIDEROAK_DIY}"

export PANDORA_DATABASE_HOST="localhost"

export SPIDEROAK_MULTI_NODE_NAME="node-sim-00"
export RABBITMQ_NODENAME="${SPIDEROAK_MULTI_NODE_NAME}"
export RABBITMQ_NODE_IP_ADDRESS="127.0.0.1"
export RABBITMQ_NODE_PORT="6000"

export DIY_NODE_EXCHANGES="node-sim-00-exchange node-sim-01-exchange node-sim-02-exchange node-sim-03-exchange node-sim-04-exchange node-sim-05-exchange node-sim-06-exchange node-sim-07-exchange node-sim-08-exchange node-sim-09-exchange"

rm /var/log/pandora/diyapi_web_server.log
${TOOL}/bin/python "${SPIDEROAK_DIY}/diyapi_web_server/diyapi_web_server_main.py" "$@"