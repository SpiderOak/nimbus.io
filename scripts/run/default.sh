#!/bin/bash

NODE_CONFIG="/etc/default/nimbus.io"

test -f $NODE_CONFIG || (echo "$NODE_CONFIG does not exist"; exit 0)
. $NODE_CONFIG

source $NIMBUSIO_SRC_PATH/scripts/run/header.sh
source $NIMBUSIO_SRC_PATH/scripts/run/exec.sh
