#!/usr/bin/env bash
# This script needs to be run with the shared python3 maui virtual env activated!

name=${1?Error: "A environment name has to be given"}
conf_file=${2?Error: "A config file path has to be provided"}

# Load all the required value from the json config
base_path=`jq -r '.base_environments_path' ${conf_file}`
est_models_src=`jq -r '.estimation_model_src' ${conf_file}`

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

# Setting up workfow, qcore and IM calc
echo "Cloning workflow"
git clone git@github.com:ucgmsim/slurm_gm_workflow.git
mv ./slurm_gm_workflow ./workflow

# tmp
cd workflow
git checkout QSW_967
cd ../

# Create workflow config
python ./workflow/install_workflow/create_config_file.py ${env_path}

# Create version
echo "dev" > ${env_path}/workflow/version

# Copy the estimation models
echo "Copying estimation models from $est_models_src"
mkdir ./workflow/estimation/models/
cp -r ${est_models_src}/* ./workflow/estimation/models/

echo "Cloning qcore"
git clone git@github.com:ucgmsim/qcore.git

echo "Cloning IM_calculation"
git clone git@github.com:ucgmsim/IM_calculation.git

echo "Cloning Pre-processing"
git clone git@github.com:ucgmsim/Pre-processing.git

echo "Cloning Empirical Engine"
git clone git@github.com:ucgmsim/Empirical_Engine.git

# Run setup for IM_calculation
echo "Running setup for IM_calculation"
cd IM_calculation
python setup_rspectra.py build_ext --inplace
cd ../


# Create virtual environment
mkdir virt_envs
python3 -m venv virt_envs/python3_maui

# Activate new python env
source ./virt_envs/python3_maui/bin/activate

# Sanity check
if [[ `which python` != *"${name}"* && `which pip` != *"${name}"* ]]; then
    echo "Something went wrong, the current python used is not from the new virtual
    environment. Quitting"
    exit
fi

# Install python packages
# Using xargs means that each package is installed individually, which
# means that if there is an error (i.e. can't find qcore), then the other
# packages are still installed. However, this is slower.
xargs -n 1 -a ${env_path}/workflow/install_workflow/maui_python3_requirements.txt pip install

# Install qcore
pip install ./qcore

