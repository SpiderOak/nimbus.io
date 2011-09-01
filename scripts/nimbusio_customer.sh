#!/bin/bash
NIMBUSIO="${HOME}/spideroak_diyapi"
export PYTHONPATH="${NIMBUSIO}"
export NIMBUSIO_CENTRAL_USER_PASSWORD="pork"
python "${NIMBUSIO}/nimbusio_customer/nimbusio_customer_main.py" "$1" "$2"

