#!/bin/bash
export TRUNK="${HOME}/spideroak_diyapi"
export PYTHONPATH="${TRUNK}"

export SPIDEROAK_MULTI_NODE_NAME="cluster-01"

python "${TRUNK}/unit_tests/test_all.py"

