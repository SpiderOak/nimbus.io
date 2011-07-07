#!/bin/bash
source ${TOOL}/bin/env.sh
export SPIDEROAK_DIY="${HOME}/spideroak_diyapi"
export PYTHONPATH="${SPIDEROAK_DIY}"
export PANDORA_DB_PW_diyapi="0.332009188365191"
export PANDORA_DB_PW_pandora_storage_server="0.383047103416175"
export PANDORA_DB_PW_pandora="0.383047103416175"
export PANDORA_DB_PW_diyapi_auditor="pork"

rm /var/log/pandora/diy_sim.log
python "${SPIDEROAK_DIY}/test/diy_sim/diy_sim_main.py"
