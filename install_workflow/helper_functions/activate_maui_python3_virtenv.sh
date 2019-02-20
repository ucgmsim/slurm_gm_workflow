#!/usr/bin/env bash
# Activates the specified python3 virtual environment.
# Note: Resets the PHYTONPATH
# Created as a separate script to allow it to be called from scripts.
virtenv_path=$1

# Load python3 module
module load Python/3.6.3-gimkl-2017a

# Reset the PYTHONPATH
export PYTHONPATH=''

# Load the virtual environment
source ${virtenv_path}/bin/activate

