#!/bin/bash
export SPIDEROAK_DIY="${HOME}/spideroak_diyapi"
export PYTHONPATH="${SPIDEROAK_DIY}"

export PANDORA_DATABASE_HOST="localhost"

export SPIDEROAK_MULTI_NODE_NAME="node-sim-00"
export SPIDEROAK_MULTI_NODE_NAME_SEQ="node-sim-00 node-sim-01 node-sim-02 node-sim-02 node-sim-04 node-sim-05 node-sim-06 node-sim-07 node-sim-08 node-sim-09"
export DIYAPI_DATABASE_SERVER_ADDRESSES="tcp://127.0.0.1:8000 tcp://127.0.0.1:8001 tcp://127.0.0.1:8002 tcp://127.0.0.1:8003 tcp://127.0.0.1:8004 tcp://127.0.0.1:8005 tcp://127.0.0.1:8006 tcp://127.0.0.1:8007 tcp://127.0.0.1:8008 tcp://127.0.0.1:8009" 

rm /var/log/pandora/diyapi_web_server.log
python "${SPIDEROAK_DIY}/diyapi_web_server/diyapi_web_server_main.py" "$@"
