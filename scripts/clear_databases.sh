#! /bin/bash

for i in 01 02 03 04 05 06 07 08 09 10
do
   psql -d "diy.multi-node-$i" -f "${HOME}/spideroak_diyapi/sql/clear_database.sql"
done

