#!/bin/bash
set -x
set -e

LEVEL="$1" 
BASEDIR="$2"

if [ ! -d $BASEDIR ]; then
    echo "basedir '$BASEDIR' does not exist"
    exit 1
fi

PYTHON="python2.7"

# pull in environment settings from the simulated cluster 
pushd "${BASEDIR}/config"
source node_01_config.sh
popd

"${PYTHON}" test/event_subscriber.py "${LEVEL}" "${BASEDIR}"
