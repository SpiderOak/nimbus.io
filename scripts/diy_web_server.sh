#!/bin/bash
export SPIDEROAK_DIY="${HOME}/spideroak_diyapi"
export PYTHONPATH="${SPIDEROAK_DIY}"

export PANDORA_DATABASE_HOST="localhost"

export SPIDEROAK_MULTI_CLUSTER_NAME="multi-node-cluster"
export SPIDEROAK_MULTI_NODE_NAME="multi-node-01"
export DIYAPI_WEB_SERVER_PIPELINE_ADDRESS="tcp://127.0.0.1:8900" 
export SPIDEROAK_MULTI_NODE_NAME_SEQ="multi-node-01 multi-node-02 multi-node-03 multi-node-04 multi-node-05 multi-node-06 multi-node-07 multi-node-08 multi-node-09 multi-node-10"
export DIYAPI_DATA_WRITER_ADDRESSES="tcp://127.0.0.1:8100 tcp://127.0.0.1:8101 tcp://127.0.0.1:8102 tcp://127.0.0.1:8103 tcp://127.0.0.1:8104 tcp://127.0.0.1:8105 tcp://127.0.0.1:8106 tcp://127.0.0.1:8107 tcp://127.0.0.1:8108 tcp://127.0.0.1:8109" 
export DIYAPI_DATA_READER_ADDRESSES="tcp://127.0.0.1:8300 tcp://127.0.0.1:8301 tcp://127.0.0.1:8302 tcp://127.0.0.1:8303 tcp://127.0.0.1:8304 tcp://127.0.0.1:8305 tcp://127.0.0.1:8306 tcp://127.0.0.1:8307 tcp://127.0.0.1:8308 tcp://127.0.0.1:8309" 
export DIYAPI_SPACE_ACCOUNTING_SERVER_ADDRESS="tcp://127.0.0.1:8500"
export DIYAPI_SPACE_ACCOUNTING_PIPELINE_ADDRESS="tcp://127.0.0.1:8550"
export DIYAPI_EVENT_PUBLISHER_PULL_ADDRESS="ipc:///tmp/spideroak-event-publisher-multi-node-01/socket"
export PANDORA_DB_PW_diyapi="1.332009188365191"
export PANDORA_DB_PW_pandora="1.332009188365191"

rm /var/log/pandora/diyapi_web_server.log
python "${SPIDEROAK_DIY}/diyapi_web_server/diyapi_web_server_main.py" "$@"
