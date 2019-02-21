#!/usr/bin/env bash
# Activate a HPC environment
env_path=${1?Error: "A valid environment path has to be given, e.g. */Environments/name/"}

# Load virtual environment
source activate_maui_python3_virtenv.sh ${env_path}/python_virtenv

# PYTHONPATH (this can be removed once qcore is installed as a pip package)
export PYTHONPATH=$PYTHONPATH:${env_path}/qcore

# PYTHONPATH for workflow
export PYTHONPATH=$PYTHONPATH:${env_path}/workflow

gmsim=${env_path}
