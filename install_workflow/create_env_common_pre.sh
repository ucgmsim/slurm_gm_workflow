#!/usr/bin/env bash

name=${1?Error: "A environment name has to be given"}
conf_file=${2?Error: "A config file path has to be provided"}

# Load all the required value from the json config
base_path=`jq -r '.base_environments_path' ${conf_file}`

# Check that this name isn't already taken.
env_path=${base_path}/${name}
if [[ -d "${env_path}" ]]; then
    echo "Environment with name $name already exists. Quitting."
    exit
fi

# Create the directory
echo "Creating enviroment folder in $env_path"
mkdir ${env_path} || exit 1
cd ${env_path}