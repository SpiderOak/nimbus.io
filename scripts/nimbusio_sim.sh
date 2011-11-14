#!/bin/bash
#export NIMBUSIO="${HOME}/nimbus.io"
#export PYTHONPATH="${NIMBUSIO}"
#export NIMBUSIO_LOG_DIR="/var/log/nimbusio"
#export NIMBUSIO_CENTRAL_PASSWORD="0.332009188365191"
#export NIMBUSIO_NODE_PASSWORD="0.383047103416175"
#
#export TEST_BASE_PATH="/var/nimbusio"

# XXX: had to do these to get these passwords to work
#alter user "nimbusio_node_user.multi-node-01" with encrypted password '0.383047103416175';
#alter user "nimbusio_node_user.multi-node-02" with encrypted password '0.383047103416175';
#alter user "nimbusio_node_user.multi-node-03" with encrypted password '0.383047103416175';
#alter user "nimbusio_node_user.multi-node-04" with encrypted password '0.383047103416175';
#alter user "nimbusio_node_user.multi-node-05" with encrypted password '0.383047103416175';
#alter user "nimbusio_node_user.multi-node-06" with encrypted password '0.383047103416175';
#alter user "nimbusio_node_user.multi-node-07" with encrypted password '0.383047103416175';
#alter user "nimbusio_node_user.multi-node-08" with encrypted password '0.383047103416175';
#alter user "nimbusio_node_user.multi-node-09" with encrypted password '0.383047103416175';
#alter user "nimbusio_node_user.multi-node-10" with encrypted password '0.383047103416175';
#alter user "nimbusio_node_user.multi-node-10" with encrypted password '0.383047103416175';
#alter user "nimbusio_central_user" with encrypted password '0.332009188365191';

#rm "${NIMBUSIO_LOG_DIR}/nimbusio_sim.log"
export NIMBUSIO="$PWD"
export PYTHONPATH=$NIMBUSIO
python "${NIMBUSIO}/test/nimbusio_sim/nimbusio_sim_main.py"

