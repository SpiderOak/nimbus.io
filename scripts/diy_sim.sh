#!/bin/bash
export TOOL="/opt/so2.6"
source ${TOOL}/bin/env.sh
export SPIDEROAK_DIY="${HOME}/spideroak_diyapi"
export PYTHONPATH="${SPIDEROAK_DIY}"

rm /var/log/pandora/diy_sim.log
${TOOL}/bin/python "${SPIDEROAK_DIY}/test/diy_sim/diy_sim_main.py"
