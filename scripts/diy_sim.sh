#!/bin/bash
source ${TOOL}/bin/env.sh
export SPIDEROAK_DIY="${HOME}/spideroak_diyapi"
export PYTHONPATH="${SPIDEROAK_DIY}"

rm /var/log/pandora/diy_sim.log
python "${SPIDEROAK_DIY}/test/diy_sim/diy_sim_main.py"
