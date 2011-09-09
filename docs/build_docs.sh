#!/bin/bash
CODEBASE="${HOME}/nimbus.io"
export PYTHONPATH="${CODEBASE}"

pushd "${CODEBASE}/docs"
make html
popd
