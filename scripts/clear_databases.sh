#! /bin/bash

for i in 01 02 03 04 05 06 07 08 09 10
do
   psql -d "diy.multi-node-$i" -f "${HOME}/spideroak_diyapi/sql/clear_local_database.sql"
done
psql -d "diy_central" -f "${HOME}/spideroak_diyapi/sql/clear_central_database.sql"
psql -d "diy_central" -f "${HOME}/spideroak_diyapi/sql/populate_test_cluster.sql"

