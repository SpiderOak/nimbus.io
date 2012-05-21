#! /bin/bash

export NIMBUSIO="${HOME}/git/nimbus.io"
for i in 01 02 03 04 05 06 07 08 09 10
do
   psql -d "nimbusio_node.multi-node-$i" -f "${NIMBUSIO}/sql/clear_nimbusio_node.sql"
done
psql -d "nimbusio_central" -f "${NIMBUSIO}/sql/clear_nimbusio_central.sql"
psql -d "nimbusio_central" -f "${NIMBUSIO}/sql/populate_nimbusio_central_test_cluster.sql"

