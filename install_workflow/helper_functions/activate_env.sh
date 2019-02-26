#!/usr/bin/env bash
# Activate a HPC environment
env_path=${1?Error: "A valid environment path has to be given, e.g. */Environments/name/"}
hpc=${2?Error: "A valid  HPC has to be specified, either maui or mahuika"}


# Load virtual environment
if [[ $2 == "maui" ]]; then
    source activate_maui_python3_virtenv.sh ${env_path}/python_virtenv_maui
elif [[ $2 == "mahuika" ]]; then
    source activate_maui_python3_virtenv.sh ${env_path}/python_virtenv_maui
else
    echo "$2, invalid HPC, Quitting!"
    exit
fi

# PYTHONPATH (this can be removed once qcore is installed as a pip package)
export PYTHONPATH=$PYTHONPATH:${env_path}/qcore

# PYTHONPATH for workflow
export PYTHONPATH=$PYTHONPATH:${env_path}/workflow

gmsim=${env_path}
