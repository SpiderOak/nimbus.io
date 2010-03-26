#!/bin/bash
export TOOL="/opt/so2.6"
source ${TOOL}/bin/env.sh
export TRUNK="${HOME}/spideroak_diyapi"
export PYTHONPATH="${TRUNK}"

export SPIDEROAK_MULTI_NODE_NAME="cluster-01"

${TOOL}/bin/python "$1"

