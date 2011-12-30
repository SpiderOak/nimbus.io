#!/bin/bash
NIMBUSIO="${HOME}/nimbus.io"
NIMBUSIO_NODE_NAME_SEQ="multi-node-01 multi-node-02 multi-node-03 multi-node-04 multi-node-05 multi-node-06 multi-node-07 multi-node-08 multi-node-09 multi-node-10"

for nodename in $NIMBUSIO_NODE_NAME_SEQ 
do
    dropdb "nimbusio_node.${nodename}"
    dropuser "nimbusio_node_user.${nodename}"
    createuser --login  --no-superuser --no-createdb --no-createrole "nimbusio_node_user.${nodename}" 
    createdb --owner="nimbusio_node_user.${nodename}" "nimbusio_node.${nodename}"
    psql -d "nimbusio_node.${nodename}" -U "nimbusio_node_user.${nodename}" -f "${NIMBUSIO}/sql/nimbusio_node.sql"      
done


