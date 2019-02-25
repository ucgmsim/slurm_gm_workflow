
### Functions to load HPC environments

activate_env () {
    source /nesi/project/nesi00213/workflow/install_workflow/helper_functions/activate_env.sh $1
}

deactivate_env () {
    source /nesi/project/nesi00213/workflow/install_workflow/helper_functions/deactivate_env.sh
}
