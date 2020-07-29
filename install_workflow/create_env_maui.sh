#!/usr/bin/env bash

my_dir="$(dirname "$0")"
source "$my_dir/create_env_common_pre.sh"

# Setting up workfow, qcore and IM calc
echo "Cloning workflow"
git clone git@github.com:ucgmsim/slurm_gm_workflow.git
mv ./slurm_gm_workflow ./workflow

# Create version
echo "dev" > ${env_path}/workflow/version

echo "Cloning qcore"
git clone git@github.com:ucgmsim/qcore.git

echo "Cloning IM_calculation"
git clone git@github.com:ucgmsim/IM_calculation.git

echo "Cloning Pre-processing"
git clone git@github.com:ucgmsim/Pre-processing.git

echo "Cloning Empirical Engine"
git clone git@github.com:ucgmsim/Empirical_Engine.git

echo "Cloning visualization"
git clone git@github.com:ucgmsim/visualization.git

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

source "$my_dir/create_env_common_post.sh"
