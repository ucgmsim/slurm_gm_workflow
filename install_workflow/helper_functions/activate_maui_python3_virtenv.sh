#!/usr/bin/env bash
# Activates the specified python3 virtual environment.
# Note: Resets the PHYTONPATH
# Created as a separate script to allow it to be called from scripts.
virtenv_path=$1

# Load python3 module
module load cray-python/3.6.5.1

# Removed python2
module del cray-python/2.7.15.1

# Reset the PYTHONPATH
export PYTHONPATH=/opt/python/3.6.5.1/lib/python3.6/site-packages
export PATH=$PATH:/nesi/project/nesi00213/opt/maui/ffmpeg_build/bin:/nesi/project/nesi00213/opt/maui/gmt/5.4.4/bin

# Load the virtual environment
source ${virtenv_path}/bin/activate

