#!/usr/bin/env bash
# Activate a HPC environment
env_path=${1?Error: "A valid environment path has to be given, e.g. */Environments/name/"}
hpc=${2?Error: "A valid  HPC has to be specified, maui, mahuika, stampede and nurion are currently available"}

# Load virtual environment
if [[ $2 == "maui" ]]; then
    source ${env_path}/workflow/workflow/environments/helper_functions/activate_maui_python3_virtenv.sh ${env_path}/virt_envs/python3_maui
elif [[ $2 == "mahuika" ]]; then
    source ${env_path}/workflow/workflow/environments/helper_functions/activate_mahuika_python3_virtenv.sh ${env_path}/virt_envs/python3_mahuika
elif [[ $2 == "stampede" ]]; then
    source ${env_path}/workflow/workflow/environments/helper_functions/activate_stampede_python3_virtenv.sh ${env_path}/virt_envs/python3_stampede
 elif [[ $2 == "nurion" ]]; then
    source ${env_path}/workflow/workflow/environments/helper_functions/activate_nurion_python3_virtenv.sh ${env_path}/virt_envs/python3_nurion
else
    echo "$2, invalid HPC, Quitting!"
    # exit
fi

# source modules required for tools if exist
module_requirments=$(python -c "from qcore.config import module_requirments; print(module_requirments)")
if [[ -f $module_requirments ]];then
    source $module_requirments
else
    echo "no module requirements found at $module_requirments, binaries may not run properly"
fi

# PYTHONPATH for workflow/workflow
export PYTHONPATH=${env_path}/workflow/workflow:$PYTHONPATH

export CUR_ENV=${env_path}
export CUR_HPC=${hpc}
export gmsim=${env_path}
