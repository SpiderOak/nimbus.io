#!/bin/bash

SPIDEROAK_MULTI_NODE_NAME_SEQ="multi-node-01 multi-node-02 multi-node-03 multi-node-04 multi-node-05 multi-node-06 multi-node-07 multi-node-08 multi-node-09 multi-node-10"

for nodename in $SPIDEROAK_MULTI_NODE_NAME_SEQ 
do
    dropdb "nimbusio_node.${nodename}"
    createdb "nimbusio_node.${nodename}"
    psql -d "nimbusio_node.${nodename}" -f "${HOME}/spideroak_diyapi/sql/nimbusio_node.sql"      
done


