#!/usr/bin/env bash

# Removed python2
module del cray-python/2.7.15.1

# Load python3 module
module load cray-python/3.6.5.1

# Activates the specified python3 virtual environment.
# Note: Resets the PHYTONPATH
# Created as a separate script to allow it to be called from scripts.
source "${env_path}/workflow/workflow/environments/helper_functions/activate_common_env.sh"

