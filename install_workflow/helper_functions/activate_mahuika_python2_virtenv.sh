#!/usr/bin/env bash
# Activates the specified python2 virtual environment.
# Note: Resets the PHYTONPATH
# Created as a separate script to allow it to be called from scripts.
virtenv_path=$1

# Load python2, have to do this as virtualenv points to this python
# verions, which is not accessible without loading
module load Python/2.7.14-gimkl-2017a

# Reset the PYTHONPATH
export PYTHONPATH=''
export PATH=$PATH:/nesi/project/nesi00213/opt/mahuika/ffmpeg_build/bin:/nesi/project/nesi00213/opt/mahuika/gmt/5.4.4/bin
# Load the virtual environment
source ${virtenv_path}/bin/activate

