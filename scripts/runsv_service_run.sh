#!/bin/bash

export NIMBUS_IO_SERVICE_DIR="$(cd $(dirname $0); pwd)"
export NIMBUS_IO_SERVICE_NAME="$(basename $NIMBUS_IO_SERVICE_DIR)"

NODE_CONFIG="/etc/default/nimbus.io"

test -f $NODE_CONFIG || {
    echo "$NODE_CONFIG does not exist"
    exit 1
}

source $NODE_CONFIG

if [[ -z "$NIMBUS_IO_SRC_PATH" ]]; then
    echo "NIMBUS_IO_SRC_PATH not set"
    exit 1
fi

if [[ -z "$PYTHONPATH" ]]; then
    export PYTHONPATH="$NIMBUS_IO_SRC_PATH"
fi

if [[ -z "$PYTHON2" ]]; then
    PYTHON2=python2.7
fi

if [[ -z "$PYTHON3" ]]; then
    PYTHON3=python3.2
fi

prog_dir="$NIMBUS_IO_SRC_PATH/$NIMBUS_IO_RUN_SERVICE_NAME"
prog_script="${NIMBUS_IO_RUN_SERVICE_NAME}_run.sh"
prog_name="${NIMBUS_IO_RUN_SERVICE_NAME}_main.py"

# if there's a bash run script, just exec that.
if [[ -e "$prog_script" ]]; then
    exec chpst -u $NIMBUS_IO_USERNAME:$NIMBUS_IO_GROUPNAME $prog_script
fi

py_interpreter="$PYTHON2"
head -n 1 "$py_script" | grep -q python3 && py_interpreter="$PYTHON3"

exec chpst \
    -u $NIMBUS_IO_USERNAME:$NIMBUS_IO_GROUPNAME \
    $py_interpreter $prog_name
