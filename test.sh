#!/bin/bash 


export PATH="/usr/lib/postgresql/9.0/bin:/usr/loca/bin:$PATH"
export SIM_DIR="/tmp/clustersim"
export PYTHONPATH=$PWD
export BASE_PORT=9000

# NIMBUSIO_LOG_DIR=/tmp/test TEST_BASE_PATH=/tmp/test 

if [ -d $SIM_DIR ]; then
    rm -rf $SIM_DIR 
fi

python test/nimbusio_sim/nimbusio_sim_main.py \
    --basedir $SIM_DIR --baseport $BASE_PORT \
    --create --logprune --start

# --systemdb
