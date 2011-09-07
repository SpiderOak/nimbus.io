#!/bin/bash
NIMBUSIO="${HOME}/nimbusio"
DATABASE_NAME="nimbusio_central"
USER_NAME="nimbusio_central_user"

dropdb "${DATABASE_NAME}"
dropuser "${USER_NAME}"
createuser --login  --no-superuser --no-createdb --no-createrole "${USER_NAME}" 
createdb --owner="${USER_NAME}" "${DATABASE_NAME}"
psql -d "${DATABASE_NAME}" -U "${USER_NAME}" -f "${NIMBUSIO}/sql/nimbusio_central.sql"
psql -d "${DATABASE_NAME}" -U "${USER_NAME}" -f "${NIMBUSIO}/sql/populate_nimbusio_central_test_cluster.sql"

