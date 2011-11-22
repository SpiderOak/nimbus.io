#!/bin/bash
export NIMBUSIO="${HOME}/nimbus.io"
export PYTHONPATH="${NIMBUSIO}"
export NIMBUSIO_LOG_DIR="/var/log/nimbusio"

export NIMBUSIO_CLUSTER_NAME="multi-node-cluster"
export NIMBUSIO_NODE_NAME="multi-node-01"
export NIMBUSIO_WEB_SERVER_PIPELINE_ADDRESS="tcp://127.0.0.1:9000" 
export NIMBUSIO_NODE_NAME_SEQ="multi-node-01 multi-node-02 multi-node-03 multi-node-04 multi-node-05 multi-node-06 multi-node-07 multi-node-08 multi-node-09 multi-node-10"
export NIMBUSIO_DATA_WRITER_ADDRESSES="tcp://127.0.0.1:8100 tcp://127.0.0.1:8101 tcp://127.0.0.1:8102 tcp://127.0.0.1:8103 tcp://127.0.0.1:8104 tcp://127.0.0.1:8105 tcp://127.0.0.1:8106 tcp://127.0.0.1:8107 tcp://127.0.0.1:8108 tcp://127.0.0.1:8109" 
export NIMBUSIO_DATA_READER_ADDRESSES="tcp://127.0.0.1:8300 tcp://127.0.0.1:8301 tcp://127.0.0.1:8302 tcp://127.0.0.1:8303 tcp://127.0.0.1:8304 tcp://127.0.0.1:8305 tcp://127.0.0.1:8306 tcp://127.0.0.1:8307 tcp://127.0.0.1:8308 tcp://127.0.0.1:8309" 
export NIMBUSIO_SPACE_ACCOUNTING_SERVER_ADDRESS="tcp://127.0.0.1:8500"
export NIMBUSIO_SPACE_ACCOUNTING_PIPELINE_ADDRESS="tcp://127.0.0.1:8550"
export NIMBUSIO_EVENT_PUBLISHER_PULL_ADDRESS="ipc:///tmp/nimbusio-event-publisher-multi-node-01/socket"
export NIMBUSIO_CENTRAL_USER_PASSWORD="1.332009188365191"
export NIMBUSIO_NODE_USER_PASSWORD="1.332009188365191"

rm "${NIMBUSIO_LOG_DIR}/nimbusio_web_server.log"
rm "${NIMBUSIO_LOG_DIR}/nimbusio_web_server_wsgi.log"
bash -c 'ulimit -H -n 32768 ; chpst -u dougfort:dougfort -o 32768 python "${NIMBUSIO}/web_server/web_server_main.py"' "$@" &> /var/log/nimbusio/nimbusio_web_server_wsgi.log

