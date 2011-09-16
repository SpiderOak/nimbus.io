#!/bin/bash
NIMBUSIO="${HOME}/nimbus.io"
export PYTHONPATH="${NIMBUSIO}"
export NIMBUSIO_CENTRAL_USER_PASSWORD="pork"
python "${NIMBUSIO}/customer/customer_main.py" "$1" "$2" "$3" "$4"

