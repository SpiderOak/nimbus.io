#!/bin/bash

# simple script to run psql for the node local database
# pass basedir of cluster sim as $1
# finds config for cluster under basedi

set -x
set -e

BASEDIR=$1
NODE_NUMBER=$2

if [ ! -d $BASEDIR ]; then
    echo "basedir '$BASEDIR' does not exist"
    exit 1
fi

# pull in environment settings from the simulated cluster 
pushd "${BASEDIR}/config"
source "node_${NODE_NUMBER}_config.sh"
popd

export PGPASSWORD="${NIMBUSIO_NOSE_USER_PASSORD}"

psql -d "nimbusio_node.${NIMBUSIO_NODE_NAME}" \
     -U "nimbusio_node_user.${NIMBUSIO_NODE_NAME}" \
     -h "${NIMBUSIO_NODE_DATABASE_HOST}" \
     -p "${NIMBUSIO_NODE_DATABASE_PORT}"


