#!/bin/bash

if [[ $# -lt 3 ]];then
    echo "please provide the list of vms to install, and path to cybershake folder"
    echo "install_cybershake.sh /path/to/cybershake/root/ /path/to/cybershake/config /path/to/cybershake/nhm_selection_filel"
    exit 1
fi

script_location=`dirname $0`

cybershake_root=$1

sim_root_dir=$cybershake_root/Runs
vm_root_dir=$cybershake_root/Data/VMs

cybershake_cfg=$2

#get list of VM
#cd $vm_root_dir
fault_list=`head $3`

#each vm match with multiple srf
IFS=$'\n'
for line in $fault_list;
do
    fault=`echo $line | awk '{print $1}'`
    n_rel=`echo $line | awk '{print $2}'`
    echo python $script_location/install_cybershake.py $cybershake_root $cybershake_cfg $fault --n_rel ${n_rel//r}
done

#list_source='ls 
#echo $sim_root_dir
