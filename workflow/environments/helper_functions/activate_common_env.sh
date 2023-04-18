#!/usr/bin/env bash
# Activates the specified python3 virtual environment.
# Note: Resets the PYTHONPATH
# Created as a separate script to allow it to be called from scripts.
virtenv_path=$1

# Reset the PYTHONPATH
export PYTHONPATH=''

# Load the virtual environment
source ${virtenv_path}/bin/activate