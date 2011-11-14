#!/bin/bash
NIMBUSIO="${HOME}/nimbus.io"
DATABASE_NAME="nimbusio_central"
USER_NAME="nimbusio_central_user"

# XXX: only runs correctly if run with sudo -u postgres on ubuntu and local pg
# set to trust

# XXX: should warn somehow we're about to be destructive

# XXX: we should check if these exist
#dropdb "${DATABASE_NAME}"
#dropuser "${USER_NAME}"
# XXX: we don't want to ignore errors after this
set -e

# XXX: i had to prefix these two with 'sudo -u postgres' 
#sudo -u postgres createuser --login  --no-superuser --no-createdb --no-createrole "${USER_NAME}" 
#sudo -u postgres createdb --owner="${USER_NAME}" "${DATABASE_NAME}"
createuser --login  --no-superuser --no-createdb --no-createrole "${USER_NAME}" 
createdb --owner="${USER_NAME}" "${DATABASE_NAME}"

psql -d "${DATABASE_NAME}" -U "${USER_NAME}" -f "${NIMBUSIO}/sql/nimbusio_central.sql"
psql -d "${DATABASE_NAME}" -U "${USER_NAME}" -f "${NIMBUSIO}/sql/populate_nimbusio_central_test_cluster.sql"

