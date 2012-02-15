#!/bin/bash

# simple script to run the commanline program against a running cluster sim.
# pass basedir of cluster sim as $1
# so $2, $3... become $1, $2... for nio_cmd

#set -x
set -e

BASEDIR=$1

if [ ! -d $BASEDIR ]; then
    echo "basedir '$BASEDIR' does not exist"
    exit 1
fi

# pull in environment settings from the simulated cluster 
source $BASEDIR/config/client_config.sh

export MOTOBOTO_IDENTITY="${NIMBUS_IO_CLIENT_PATH}/motoboto-benchmark-039"

nio_cmd $2 $3 $4

exit $?

