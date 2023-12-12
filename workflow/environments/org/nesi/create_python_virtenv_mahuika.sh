#!/usr/bin/env bash
# Creates the virtual environment on mahuika for the specified environment
# This has to be run from mahuika, after create_env.sh has been run on maui!

env_path=${1?Error: "The environment path has to be given"}
name=`basename ${env_path}`
#get the absolute path of this script
DIR=$( dirname "$( realpath "${BASH_SOURCE[0]}")" )

inhouse_pkgs=(qcore IM_calculation Pre-processing Empirical_Engine visualization) #TODO: rename slurm_gm_workflow to workflow and add here


# Create virtual environment
cd ${env_path}

which python
echo "===================================================="
echo "^ this must be coming from the correct python module"

python3 -m venv --system-site-packages virt_envs/python3_mahuika

# Activate new python env
source ./virt_envs/python3_mahuika/bin/activate
which python
echo "===================================================="
echo "^ this must be coming from the virtual env just created"
python --version
echo "===================================================="
echo "^ double check the python version"


# make sure we install a copy of pip in the virtual env
python -m pip install --upgrade pip #pip install --upgrade pip
which pip 
echo "===================================================="
echo "^ double check  pip is coming from the new virt_env"

# Sanity check
if [[ `which python` != *"${name}"* && `which pip` != *"${name}"* ]]; then
    echo "Something went wrong, the current python used is not from the new virtual
    environment. Quitting"
    exit
fi

pip install --upgrade setuptools
pip install wheel==0.36.2
# Install python packages
# Using xargs means that each package is installed individually, which
# means that if there is an error (i.e. can't find qcore), then the other
# packages are still installed. However, this is slower.
xargs -n 1 -a $DIR/nesi/mahuika_python3_requirements.txt pip install -U

inhouse_pkgs+=(oq-engine) # add oq-engine
for pkg in "${inhouse_pkgs[@]}";
do
    cd ${env_path}/${pkg}
    pip install -U -r requirements.txt
    cd ../
    pip install -e ./${pkg}
done

#TODO: once inhouse_pkgs includes workflow, remove the following
cd workflow
pip install -U -r requirements.txt
cd ..
pip install -e ./workflow
