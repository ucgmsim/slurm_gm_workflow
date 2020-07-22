#!/usr/bin/env bash
# Activates the specified python3 virtual environment.
# Note: Resets the PHYTONPATH
# Created as a separate script to allow it to be called from scripts.
my_dir="$(dirname "$0")"
source "$my_dir/activate_common_env.sh"

# Load python3 module
module load cray-python/3.6.5.1

# Removed python2
module del cray-python/2.7.15.1

# Reset the PYTHONPATH
export PYTHONPATH=$PYTHONPATH:/opt/python/3.6.5.1/lib/python3.6/site-packages

# Load the virtual environment
source ${virtenv_path}/bin/activate

