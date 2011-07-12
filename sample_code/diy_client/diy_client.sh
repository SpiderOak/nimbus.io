#!/bin/bash
# script fpr running the client on Linux
export DIYAPI="${HOME}/spideroak_diyapi"
export PYTHONPATH="${DIYAPI}"

python "${DIYAPI}/sample_code/diy_client/diy_client_main.py"
