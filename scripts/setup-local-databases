#!/bin/bash

SPIDEROAK_MULTI_NODE_NAME_SEQ="multi-node-01 multi-node-02 multi-node-03 multi-node-04 multi-node-05 multi-node-06 multi-node-07 multi-node-07 multi-node-08 multi-node-09 multi-node-10"

for nodename in $SPIDEROAK_MULTI_NODE_NAME_SEQ 
do
    dropdb "diy.${nodename}"
    createdb "diy.${nodename}"
    psql -d "diy.${nodename}" -f "${HOME}/spideroak_diyapi/sql/node_local_database_schema.sql"      
done


