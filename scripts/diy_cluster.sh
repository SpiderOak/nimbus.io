#!/bin/bash
export SPIDEROAK_DIY="${HOME}/spideroak_diyapi"
export PYTHONPATH="${SPIDEROAK_DIY}"

rm /var/log/pandora/cluster_sim.log
python "${SPIDEROAK_DIY}/test/cluster_sim/cluster_sim_main.py"
