#!/bin/bash

#set -e
#set -x

export NIMBUS_IO_RUN_SERVICE_DIR="$(cd $(dirname $0); pwd)"

export NIMBUS_IO_RUN_SERVICE_NAME="$(basename $NIMBUS_IO_RUN_SERVICE_DIR)"

# some sites install the service directory for each service NAME as
# /etc/service/nimbus.io.NAME, so remove "nimbus.io" from the left to find the
# name of the program to run.
export NIMBUS_IO_RUN_PROGRAM_NAME="${NIMBUS_IO_RUN_SERVICE_NAME#nimbus.io.}"

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

prog_dir="$NIMBUS_IO_SRC_PATH/$NIMBUS_IO_RUN_PROGRAM_NAME"
prog_script="${NIMBUS_IO_RUN_PROGRAM_NAME}_run.sh"
prog_file="${NIMBUS_IO_RUN_PROGRAM_NAME}_main.py"

# if there's a bash run script, we'll just exec that
if [[ -e "$prog_dir/$prog_script" ]]; then
    prog_cmd="$prog_dir/$prog_script"

# otherwise look for a python main file
elif [[ -e "$prog_dir/$prog_file" ]]; then
    prog_cmd="$prog_dir/$prog_file"
    py_interpreter="$PYTHON2"
    head -n 1 "$prog_cmd" | grep -q python3 && py_interpreter="$PYTHON3"
    prog_cmd="$py_interpreter $prog_cmd"
else
    echo "Cannot find executable"
    exit 1
fi

exec chpst \
    -u ${NIMBUS_IO_USERNAME:?}:${NIMBUS_IO_GROUPNAME:?} \
    $prog_cmd
