#!/bin/bash
NIMBUSIO="${HOME}/git/nimbus.io"
export PYTHONPATH="${NIMBUSIO}"
export NIMBUSIO_CENTRAL_USER_PASSWORD="pork"
PYTHON="python2.7"
"${PYTHON}" "${NIMBUSIO}/customer/customer_main.py" "$1" "$2" "$3" "$4"

