#!/bin/bash

#get the path to Runs from args
if [[ $# -lt 2 ]];then
    echo "please provide a path to runs and the list_vm"
    exit 1
fi
#source the cybershake/workflow path to locate where the script is called.
script_location=`dirname $0`
source $script_location/cybershake_path.sh

cwd=$1
sim_list=`cat $2`
echo "submitting EMOD3D for:"
echo "$sim_list"
echo "=============================="
for sim in $sim_list;
do
    echo "submitting for $sim"
    echo "=============================="
    cd $cwd/$sim
    python $gmsim/workflow/scripts/submit_emod3d.py --auto
    #echo 
    echo "=============================="
done



