#!/usr/bin/env bash

if [ "$#" -ne 2 ]
  then echo "This script requires two arguments: Env_name/path and enviroment config path"
  exit
fi

#get the absolute path of this script
DIR=$( dirname "$( realpath "${BASH_SOURCE[0]}")" )
# sourcing the script below uses the args passed to this script
source "${DIR}/../../create_env_common_pre.sh"

inhouse_pkgs=(qcore IM_calculation Pre-processing Empirical_Engine visualization) #TODO: rename slurm_gm_workflow to workflow and add here

# Setting up workfow, qcore and IM calc
git clone git@github.com:ucgmsim/slurm_gm_workflow.git
mv ./slurm_gm_workflow ./workflow

# Create version
echo "dev" > ${env_path}/workflow/version


for pkg in "${inhouse_pkgs[@]}";
do
    echo "Cloning $pkg"
    git clone git@github.com:ucgmsim/${pkg}.git
done

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
pip install --upgrade setuptools wheel
# Install python packages
# Using xargs means that each package is installed individually, which
# means that if there is an error (i.e. can't find qcore), then the other
# packages are still installed. However, this is slower.
xargs -n 1 -a $DIR/maui_python3_requirements.txt pip install -U

for pkg in "${inhouse_pkgs[@]}";
do
  if [ "$pkg" != "Empirical_Engine" ]; then # Empirical_Engine (and oq-engine) is only installed on Mahuika
      cd ${env_path}/${pkg}
      pip install -U -r requirements.txt
      cd ../
      pip install -e ./${pkg}
  fi
done

#TODO: once inhouse_pkgs includes workflow, remove the following
cd workflow
pip install -U -r requirements.txt
cd ../
pip install -e ./workflow
