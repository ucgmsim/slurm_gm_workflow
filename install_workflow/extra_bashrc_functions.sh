
### Functions to load HPC environments

activate_env () {
    env_path=${1?Error: "The environment path has to be provided, e.g. ..../Environments/test_env"}

    if [[ `hostname` =~ 'mahuika' ]] || [[ $HOSTNAME =~ 'wb' ]];then
        hpc='mahuika'
    elif [[ `hostname` =~ 'maui' ]] || [[ $HOSTNAME =~ 'ni' ]];then
        hpc='maui'
    else
        echo "You might be lost, or more likely this is not working as planned!"
    fi

    source ${env_path}/workflow/install_workflow/helper_functions/activate_env.sh ${env_path} ${hpc}
}

deactivate_env () {
    if [[ ! -z ${CUR_ENV} ]]; then
        source ${CUR_ENV}/workflow/install_workflow/helper_functions/deactivate_env.sh
    else
        echo "You do not appear to be in an environment? Use deactivate_virtevnv, to deactivate a python virtual env"
    fi
}
