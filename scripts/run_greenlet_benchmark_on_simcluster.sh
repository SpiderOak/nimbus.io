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
MOTOBOTO_BENCH_DIR="$CODE_PARENT/motoboto_benchmark"

if [ ! -d "$MOTOBOTO_DIR" ]; then
    echo "Did not find motoboto source in $MOTOBOTO_DIR"
    echo "You need to have source for the client libraries: lumberyard and motoboto"
    echo "Do this:"
    echo "  cd $CODE_PARENT"
    echo "  git clone https://nimbus.io/dev/source/lumberyard.git/"
    echo "  git clone https://nimbus.io/dev/source/motoboto.git/"
    echo "  cd lumberyard ; sudo python setup.py install ; cd .."
    echo "  cd motoboto ; sudo python setup.py install ; cd .."
    exit 1
fi

if [ ! -d "$MOTOBOTO_BENCH_DIR" ]; then
    echo "Did not find motoboto_benchmark source in $MOTOBOTO_BENCH_DIR"
    echo "Do this:"
    echo "  cd $CODE_PARENT"
    echo "  git clone https://nimbus.io/dev/source/motoboto_benchmark.git/"
    exit 1
fi

set -x
set -e

BASEDIR=$1

if [ ! -d $BASEDIR ]; then
    echo "basedir of simulated cluster '$BASEDIR' does not exist"
    exit 1
fi

CLIENT_PATH=$BASEDIR/client

# pull in environment settings from the simulated cluster 
source $BASEDIR/config/central_config.sh
source $BASEDIR/config/client_config.sh

# create 100 test users
for i in {0..99} ; do
    printf -v TEST_USERNAME "motoboto-benchmark-%03d" $i
    MOTOBOTO_IDENTIY="$CLIENT_PATH/$TEST_USERNAME"
    if [ ! -e $MOTOBOTO_IDENTIY ]; then
        echo "Creating user $TEST_USERNAME with config $MOTOBOTO_IDENTIY"
        python customer/customer_main.py --create-customer \
            --username=$TEST_USERNAME > $MOTOBOTO_IDENTIY
    fi 
done

export NIMBUSIO_CONNECTION_TIMEOUT=360.0

# run the benchmark
python $MOTOBOTO_BENCH_DIR/motoboto_benchmark_greenlet_main.py \
    --test-script="$SCRIPTS_DIR/motoboto_big_test_script.json" \
    --user-identity-dir="$CLIENT_PATH" \
    --max-users=100 \
    --test-duration=3600
