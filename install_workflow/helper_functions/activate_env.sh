#!/usr/bin/env bash
# Activate a HPC environment
env_path=${1?Error: "A valid environment path has to be given, e.g. */Environments/name/"}
hpc=${2?Error: "A valid  HPC has to be specified, either maui or mahuika"}

# Load virtual environment
if [[ $2 == "maui" ]]; then
    source ${env_path}/workflow/install_workflow/helper_functions/activate_maui_python3_virtenv.sh ${env_path}/virt_envs/python3_maui
    export PYTHONPATH=${env_path}/virt_envs/python3_maui/lib/python3.6/site-packages/:$PYTHONPATH
elif [[ $2 == "mahuika" ]]; then
    source ${env_path}/workflow/install_workflow/helper_functions/activate_mahuika_python3_virtenv.sh ${env_path}/virt_envs/python3_mahuika
elif [[ $2 == "stampede" ]]; then
    source ${env_path}/workflow/install_workflow/helper_functions/activate_stampede_python3_virtenv.sh ${env_path}/virt_envs/python3_stampede
 elif [[ $2 == "nurion" ]]; then
    source ${env_path}/workflow/install_workflow/helper_functions/activate_nurion_python3_virtenv.sh ${env_path}/virt_envs/python3_nurion
else
    echo "$2, invalid HPC, Quitting!"
    exit
fi

# PYTHONPATH for workflow
export PYTHONPATH=$PYTHONPATH:${env_path}/workflow

export CUR_ENV=${env_path}
export CUR_HPC=${hpc}
export gmsim=${env_path}
