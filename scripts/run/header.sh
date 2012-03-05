#!/bin/bash

export PYTHONPATH=$NIMBUSIO_SRC_PATH

run_script=$0
run_dir=$(dirname "$run_script")
run_dir=$(cd "$run_dir" ; pwd)
run_service=$(basename "$run_dir")

