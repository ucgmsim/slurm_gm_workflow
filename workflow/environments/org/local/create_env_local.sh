#!/usr/bin/env bash

if [ "$#" -ne 2 ]
  then echo "This script requires two arguments: Env_name/path and enviroment config path"
  exit
fi

#get the absolute path of this script
DIR=$( dirname "$( realpath "${BASH_SOURCE[0]}")" )
# sourcing the script below uses the args passed to this script
source "${DIR}/../../create_env_common_pre.sh"

# Setting up workfow, qcore and IM calc
echo "Cloning workflow"
git clone https://github.com/ucgmsim/slurm_gm_workflow.git
mv ./slurm_gm_workflow ./workflow

# Create version
echo "dev" > ${env_path}/workflow/version

inhouse_pkgs=(qcore IM_calculation Pre-processing Empirical_Engine visualization) #TODO: rename slurm_gm_workflow to workflow and add here
for pkg in "${inhouse_pkgs[@]}";
do
    echo "Cloning $pkg"
    git clone https://github.com/ucgmsim/${pkg}.git
done

# Create virtual environment
mkdir virt_envs
python3 -m venv --system-site-packages virt_envs/python3_local

# Activate new python env
source ./virt_envs/python3_local/bin/activate

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
xargs -n 1 -a ${env_path}/workflow/workflow/environments/org/local/local_python3_requirements.txt pip install

cd ${env_path}
cd workflow
pip install -U -r requirements.txt
cd ../
pip install -e ./workflow

for pkg in "${inhouse_pkgs[@]}";
do
    cd ${env_path}/${pkg}
    pip install -U -r requirements.txt
    cd ../
    pip install -e ./${pkg}
done
