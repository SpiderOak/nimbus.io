#!/bin/bash
dropdb "nimbusio_central"
createdb "nimbusio_central"
MULTI_CLUSTER_NAME='local_multi_cluster'
psql -d "nimbusio_central" -f "${HOME}/spideroak_diyapi/sql/nimbusio_central.sql"
psql -d "nimbusio_central" -f "${HOME}/spideroak_diyapi/sql/populate_nimbusio_central_test_cluster.sql"

