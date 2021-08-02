#!/usr/bin/env bash

#get the absolute path of this script
DIR=$( dirname "$( realpath "${BASH_SOURCE[0]}")" )
source "${DIR}/create_env_common_pre.sh"

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
# The flag --system-site-packages sets include-system-site-packages to be true in the environment and allows the provided mpi4py to be used
python3 -m venv --system-site-packages virt_envs/python3_maui

# Activate new python env
source ./virt_envs/python3_maui/bin/activate

# Sanity check
if [[ `which python` != *"${name}"* && `which pip` != *"${name}"* ]]; then
    echo "Something went wrong, the current python used is not from the new virtual
    environment. Quitting"
    exit
fi

# update pip. python3 come with a v9.0 which is too old.
pip install --upgrade pip

# Install python packages
# Using xargs means that each package is installed individually, which
# means that if there is an error (i.e. can't find qcore), then the other
# packages are still installed. However, this is slower.
xargs -n 1 -a ${env_path}/workflow/install_workflow/maui_python3_requirements.txt pip install -U

source "${env_path}/workflow/install_workflow/create_env_common_post.sh"
