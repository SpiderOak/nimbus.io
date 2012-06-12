#!/bin/bash

# simple script to run the unit tests from motoboto against a running cluster sim.
# pass basedir of cluster sim as $1
# finds config for cluster under basedir, adds test user to cluster, and runs
# motoboto tests.

# expects to be run from nimbus.io checkout, and expects motoboto to be checked
# out in ../motobobo

SCRIPTS_DIR="$(dirname $0)"
CODEBASE="$(dirname $SCRIPTS_DIR)"
CODEBASE="$(cd $CODEBASE ; pwd)"
CODE_PARENT="$(dirname $CODEBASE)"
MOTOBOTO_DIR="$CODE_PARENT/motoboto"

if [ ! -d "$MOTOBOTO_DIR" ]; then
    echo "Did not find motoboto source in $MOTOBOTO_DIR"
    echo "You need to have source for the client libraries: lumberyard and motoboto"
    echo "Do this:"
    echo "  cd $(dirname $MOTOBOTO_DIR)"
    echo "  git clone https://nimbus.io/dev/source/lumberyard.git/"
    echo "  git clone https://nimbus.io/dev/source/motoboto.git/"
    echo "  cd lumberyard ; sudo python setup.py install ; cd .."
    echo "  cd motoboto ; sudo python setup.py install ; cd .."
    exit 1
fi

set -x
set -e

BASEDIR=$1
TEST_USERNAME="motoboto-test-01"
PYTHON=python2.7

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
    "${PYTHON}" customer/customer_main.py --create-customer \
        --username=$TEST_USERNAME > $MOTOBOTO_IDENTITY
else
    echo "Using existing user $TEST_USERNAME"
fi

export PYTHONPATH="${PYTHONPATH}:${MOTOBOTO_DIR}"
pushd "${MOTOBOTO_DIR}/tests"

# run unit tests with identity file
"${PYTHON}" "test_all.py"

popd

