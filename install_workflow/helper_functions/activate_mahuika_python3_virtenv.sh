#!/usr/bin/env bash
# Activates the specified python3 virtual environment.
# Note: Resets the PHYTONPATH
# Created as a separate script to allow it to be called from scripts.
virtenv_path=$1

# Load python3, have to do this as virtualenv points to this python
# verions, which is not accessible without loading
module load Python/3.6.3-gimkl-2017a

# Reset the PYTHONPATH
export PYTHONPATH=''
export PYTH=$PYTH:/nesi/project/nesi00213/opt/mahuika/ffmpeg_build/bin
# Load the virtual environment
source ${virtenv_path}/bin/activate

