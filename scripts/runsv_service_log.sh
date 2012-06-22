#!/bin/bash

export NIMBUS_IO_RUN_SERVICE_DIR="$(dirname $(cd $(dirname $0); pwd))"

export NIMBUS_IO_RUN_SERVICE_NAME="$(basename $NIMBUS_IO_RUN_SERVICE_DIR)"

# some sites install the service directory for each service NAME as
# /etc/service/nimbus.io.NAME, so remove "nimbus.io" from the left to find the
# name of the program to run.
export NIMBUS_IO_RUN_PROGRAM_NAME="${NIMBUS_IO_RUN_SERVICE_NAME#nimbus.io.}"

NODE_CONFIG="/etc/default/nimbus.io"

# if there's a specific config file for this particular program, use that
if [[ -e "$NODE_CONFIG.$NIMBUS_IO_RUN_PROGRAM_NAME" ]]; then
    NODE_CONFIG="$NODE_CONFIG.$NIMBUS_IO_RUN_PROGRAM_NAME"
fi

test -f $NODE_CONFIG || {
    echo "$NODE_CONFIG does not exist"
    exit 1
}

source $NODE_CONFIG

LOG_DEST="${NIMBUSIO_LOG_DIR:?}/$NIMBUS_IO_RUN_PROGRAM_NAME.stdio"
if [[ ! -e "$LOG_DEST" ]]; then
    mkdir "$LOG_DEST"
fi

exec svlogd -ttt $LOG_DEST
