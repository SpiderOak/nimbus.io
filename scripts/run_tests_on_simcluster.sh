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
TEST_USERNAME="motoboto-test-01"

if [ ! -d $BASEDIR ]; then
    echo "basedir '$BASEDIR' does not exist"
    exit 1
fi

CLIENT_DIR=$BASEDIR/client

export MOTOBOTO_IDENTITY="$CLIENT_DIR/motoboto_test_user_id"

# pull in environment settings from the simulated cluster 
source $BASEDIR/config/central_config.sh
source $BASEDIR/config/client_config.sh

# create our test user and save the identify file
if [ ! -e $MOTOBOTO_IDENTITY ]; then
    echo "Creating new user $TEST_USERNAME"
    python customer/customer_main.py --create-customer \
        --username=$TEST_USERNAME > $MOTOBOTO_IDENTITY
else
    echo "Using existing user $TEST_USERNAME"
fi

# run unit tests with identity file
python ../motoboto/tests/test_s3_replacement.py
