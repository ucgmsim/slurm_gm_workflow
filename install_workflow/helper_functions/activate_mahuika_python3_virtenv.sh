#!/usr/bin/env bash
# Activates the specified python3 virtual environment.
# Note: Resets the PHYTONPATH
# Created as a separate script to allow it to be called from scripts.
virtenv_path=$1

# Currently hardcoded to default virtual environment, as creating of custom virtual
# environments is a bit of a pain. See README.
virtenv_path=/nesi/project/nesi00213/share/virt_envs/python3_mahuika

# Load python3, have to do this as virtualenv points to this python
# verions, which is not accessible without loading
module load Python/3.6.3-gimkl-2017a

# Reset the PYTHONPATH
export PYTHONPATH=''

# Load the virtual environment
source ${virtenv_path}/bin/activate

