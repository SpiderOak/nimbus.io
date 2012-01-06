#!/bin/bash 

# simple script for restarting a new test simulated cluster

export PATH="/usr/lib/postgresql/9.0/bin:/usr/loca/bin:$PATH"
export SIM_DIR="/tmp/clustersim"
export NIMBUSIO="${HOME}/nimbus.io"
export PYTHONPATH="${NIMBUSIO}"
export BASE_PORT=9000

# NIMBUSIO_LOG_DIR=/tmp/test TEST_BASE_PATH=/tmp/test 

python "${NIMBUSIO}/test/nimbusio_sim/nimbusio_sim_main.py" \
    --basedir $SIM_DIR --baseport $BASE_PORT \
    --logprune --start --singledb
# --systemdb
