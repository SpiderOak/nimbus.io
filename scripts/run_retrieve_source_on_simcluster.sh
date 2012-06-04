#!/bin/bash

# simple script to run the retrieve source against a running cluster sim.
# pass basedir of cluster sim as $1
# finds config for cluster under basedir

set -x
set -e

BASEDIR=$1

if [ ! -d $BASEDIR ]; then
    echo "basedir '$BASEDIR' does not exist"
    exit 1
fi

PYTHON="python3.2"

# pull in environment settings from the simulated cluster 
pushd "${BASEDIR}/config"
source node_05_config.sh
popd


#export NIMBUSIO_RETRIEVE_SOURCE_ADDRESS="tcp://127.0.0.1:9876"
export NIMBUSIO_LOG_LEVEL="DEBUG"

# run unit tests with identity file
$PYTHON "${HOME}/git/nimbus.io/retrieve_source/retrieve_source_main.py"

RETVAL=$?
if [ $RETVAL -eq 0 ]; then
    echo Success 
    exit 0 
fi

echo Failure
exit $?


