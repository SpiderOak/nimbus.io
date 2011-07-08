#!/bin/bash
export SPIDEROAK_DIY="${HOME}/spideroak_diyapi"
export PYTHONPATH="${SPIDEROAK_DIY}"
export SPIDEROAK_MULTI_NODE_NAME="multi-node-01"
export DIYAPI_EVENT_PUBLISHER_PUB_ADDRESS="tcp://127.0.0.1:8800"

rm /var/log/pandora/event_subscriber.log
python "${SPIDEROAK_DIY}/test/event_subscriber.py"
