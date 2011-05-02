#!/bin/bash
export SPIDEROAK_DIY="${HOME}/spideroak_diyapi"
export PYTHONPATH="${SPIDEROAK_DIY}"

export PANDORA_DATABASE_HOST="localhost"

export SPIDEROAK_MULTI_NODE_NAME="node-sim-00"
export DIYAPI_WEB_SERVER_PIPELINE_ADDRESS="tcp://127.0.0.1:8900" 
export SPIDEROAK_MULTI_NODE_NAME_SEQ="node-sim-00 node-sim-01 node-sim-02 node-sim-02 node-sim-04 node-sim-05 node-sim-06 node-sim-07 node-sim-08 node-sim-09"
export DIYAPI_DATABASE_SERVER_ADDRESSES="tcp://127.0.0.1:8000 tcp://127.0.0.1:8001 tcp://127.0.0.1:8002 tcp://127.0.0.1:8003 tcp://127.0.0.1:8004 tcp://127.0.0.1:8005 tcp://127.0.0.1:8006 tcp://127.0.0.1:8007 tcp://127.0.0.1:8008 tcp://127.0.0.1:8009" 
export DIYAPI_DATA_WRITER_ADDRESSES="tcp://127.0.0.1:8100 tcp://127.0.0.1:8101 tcp://127.0.0.1:8102 tcp://127.0.0.1:8103 tcp://127.0.0.1:8104 tcp://127.0.0.1:8105 tcp://127.0.0.1:8106 tcp://127.0.0.1:8107 tcp://127.0.0.1:8108 tcp://127.0.0.1:8109" 
export DIYAPI_DATA_READER_ADDRESSES="tcp://127.0.0.1:8300 tcp://127.0.0.1:8301 tcp://127.0.0.1:8302 tcp://127.0.0.1:8303 tcp://127.0.0.1:8304 tcp://127.0.0.1:8305 tcp://127.0.0.1:8306 tcp://127.0.0.1:8307 tcp://127.0.0.1:8308 tcp://127.0.0.1:8309" 
export DIYAPI_SPACE_ACCOUNTING_SERVER_ADDRESS="tcp://127.0.0.1:8300"
export DIYAPI_SPACE_ACCOUNTING_PIPELINE_ADDRESS="tcp://127.0.0.1:8350"
export PANDORA_DB_PW_diyapi="0.332009188365191"

rm /var/log/pandora/diyapi_web_server.log
python "${SPIDEROAK_DIY}/diyapi_web_server/diyapi_web_server_main.py" "$@"
