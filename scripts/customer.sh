#!/bin/bash
NIMBUSIO="${HOME}/nimbusio"
export PYTHONPATH="${NIMBUSIO}"
export NIMBUSIO_CENTRAL_USER_PASSWORD="pork"
python "${NIMBUSIO}/customer/customer_main.py" "$1" "$2"

