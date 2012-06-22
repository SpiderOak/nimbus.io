#!/bin/bash

# simple script to run the zfec_server against a running cluster sim.
# pass basedir of cluster sim as $1
# finds config for cluster under basedi

set -x
set -e

BASEDIR=$1

if [ ! -d $BASEDIR ]; then
    echo "basedir '$BASEDIR' does not exist"
    exit 1
fi

PYTHON="python2.7"

# pull in environment settings from the simulated cluster 
pushd "${BASEDIR}/config"
source node_01_config.sh
popd

export NIMBUSIO_LOG_LEVEL="DEBUG"
export NIMBUSIO_ZFEC_SERVER_ADDRESS="ipc:///${BASEDIR}sockets/${NIMBUSIO_NODE_NAME}.zfec_server.socket"

# run unit tests with identity file
$PYTHON "${HOME}/git/nimbus.io/zfec_server/zfec_server_main.py"

