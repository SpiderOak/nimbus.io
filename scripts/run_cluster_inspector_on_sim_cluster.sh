#!/bin/bash

# simple script to run the node inspector against a running cluster sim.
# pass basedir of cluster sim as $1
# finds config for cluster under basedi

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
source node_01_config.sh
popd

export NIMBUSIO_LOG_LEVEL="DEBUG"
export NIMBUSIO_MIN_ANTI_ENTROPY_AGE="hours=1"

$PYTHON "${HOME}/git/nimbus.io/anti_entropy/cluster_inspector/cluster_inspector_main.py"

