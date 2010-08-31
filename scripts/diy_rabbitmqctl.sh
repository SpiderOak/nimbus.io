#!/bin/bash
export TOOL="/opt/so2.6"
source ${TOOL}/bin/env.sh
export SPIDEROAK_DIY="${HOME}/spideroak_diyapi"
export PYTHONPATH="${SPIDEROAK_DIY}"

export SPIDEROAK_MULTI_NODE_NAME="node-sim-01"
export RABBITMQ_NODENAME="${SPIDEROAK_MULTI_NODE_NAME}"
export RABBITMQ_NODE_IP_ADDRESS="127.0.0.1"
export RABBITMQ_NODE_PORT="6001"

$TOOL/sbin/rabbitmqctl list_queues -p spideroak_vhost
