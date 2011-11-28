#!/bin/bash

# simple script to run the unit tests from motoboto against a running cluster sim.
# pass basedir of cluster sim as $1
# finds config for cluster under basedir, adds test user to cluster, and runs
# motoboto tests.

# expects to be run from nimbus.io checkout, and expects motoboto to be checked
# out in ../motobobo

set -x
set -e

BASEDIR=$1

if [ ! -d $BASEDIR ]; then
    echo "basedir '$BASEDIR' does not exist"
    exit 1
fi

CLIENT_DIR=$BASEDIR/clients

# pull in environment settings from the simulated cluster 
source $BASEDIR/config/central_config.sh
source $BASEDIR/config/client_config.sh

# create 1000 test users
for i in {0..999} ; do
    printf -v TEST_USERNAME "motoboto-benchmark-%03d" $i
    MOTOBOTO_IDENTIY="$CLIENT_DIR/$TEST_USERNAME"
    if [ ! -e $MOTOBOTO_IDENTIY ]; then
        echo "Creating user $TEST_USERNAME with config $MOTOBOTO_IDENTIY"
        python customer/customer_main.py --create-customer \
            --username=$TEST_USERNAME > $MOTOBOTO_IDENTIY
    fi 
done

# run the benchmark
python ../motoboto_benchmark/motoboto_benchmark_main.py \
    --test-script="$HOME/motoboto_big_test_script.json" \
    --user-identity-dir="$CLIENT_DIR" \
    --max-users=100 \
    --test-duration=1800
