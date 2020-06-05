#!/usr/bin/env bash

if [ $# -lt 2 ]; then
  echo "Please provide a ROOT installation directory, and the version code"
  exit 1
fi
ROOT=$1
version=$2
script_dir=`dirname "$0"`
function test_dir {
    [ -d $1 ] && echo "$1 is available" || echo "Please get a version of the $1 library later on $2"
}
function print_message {
    echo "***************"
    echo "$1"
    echo "***************"
}

# check for directories that are needed on $ROOT
test_dir ${ROOT}/EMOD3D ${ROOT}
test_dir ${ROOT}/qcore ${ROOT}

echo "Preparing the necessary directories to run GM"
mkdir -p -v ${ROOT}/{VelocityModel,VelocityModels,StationInfo,workflow,RunFolder,RupModel,share}
chmod g+w ${ROOT}/{VelocityModel,VelocityModels,StationInfo,workflow,RunFolder,RupModel,share}

# preparing the file bashrc.uceq
mv ${ROOT}/share/bashrc.uceq ${ROOT}/share/bashrc.uceq_`date +%Y%m%d_%H%M%S`
touch ${ROOT}/share/bashrc.uceq

echo "export gmsim='$ROOT'" >> ${ROOT}/share/bashrc.uceq
echo "export nobackup='/nesi/nobackup/nesi00213'" >> ${ROOT}/share/bashrc.uceq
echo 'export PATH=$PATH:'${ROOT}/workflow/scripts >> ${ROOT}/share/bashrc.uceq
echo "export PYTHONPATH=$ROOT/workflow:"'$PYTHONPATH' >> ${ROOT}/share/bashrc.uceq

#TODO:tempory disabled chgrp because its breaking the remote daemon. fix this if possible
#cat $script_dir/change_grp.sh >> ${ROOT}/share/bashrc.uceq
cat $script_dir/extra_bashrc_functions.sh >> ${ROOT}/share/bashrc.uceq
cat $script_dir/python_load_functions.sh >> ${ROOT}/share/bashrc.uceq
cat $script_dir/load_default_modules.sh >> ${ROOT}/share/bashrc.uceq


print_message "Add source $ROOT/share/bashrc.uceq to your .bashrc"


# copying the files to the workflow
cp -r $script_dir/../scripts ${ROOT}/workflow/
cp -r $script_dir/../templates ${ROOT}/workflow/
cp -r $script_dir/../shared_workflow ${ROOT}/workflow/
rsync -r $script_dir/../estimation ${ROOT}/workflow/ --exclude models
cp -r $script_dir/../metadata ${ROOT}/workflow/

#create a file that contains version code for tracking
echo $version >> ${ROOT}/workflow/version

touch ../scripts ${ROOT}/workflow/{scripts,templates,shared_workflow}/__init__.py

# Edit the machine_env.sh file to have the correct paths
print_message "Remember to edit the $ROOT/workflow/templates/machine_env.sh to fit your current system"

rm -f ${ROOT}/workflow/templates/machine_env.sh
touch ${ROOT}/workflow/templates/machine_env.sh

#script to change grp
#cat $script_dir/change_grp.sh >> ${ROOT}/workflow/templates/machine_env.sh

echo "" >> ${ROOT}/workflow/templates/machine_env.sh
echo "source $ROOT/share/bashrc.uceq" >> ${ROOT}/workflow/templates/machine_env.sh
echo "export BINPROCESS=$ROOT/workflow/scripts" >> ${ROOT}/workflow/templates/machine_env.sh
echo "" >> ${ROOT}/workflow/templates/machine_env.sh

print_message "Remember to edit the $ROOT/workflow/templates/slurm_header.cfg with the data needed on your system"

