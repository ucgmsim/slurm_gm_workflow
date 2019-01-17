#!/bin/bash

if [[ $# -lt 4 ]];then
    echo "please provide the list of vms to install, path to cybershake folder, and gmsim version"
    echo "install_cybershake.sh /path/to/cybershake/root/ /path/to/cybershake/config /path/to/list_vms version"
    exit 1
fi

script_location=`dirname $0`

cybershake_root=$1

sim_root_dir=$cybershake_root/Runs
vm_root_dir=$cybershake_root/Data/VMs

cybershake_cfg=$2

#get list of VM
#cd $vm_root_dir
list_vm=`cat $3`
echo $list_vm

#gmsim version,eg.16.1
version=$4
echo $version

#each vm match with multiple srf
for vm in $list_vm;
do
    python $script_location/install_cybershake.py $cybershake_root $cybershake_cfg $vm --version $version
done

#list_source='ls 
#echo $sim_root_dir
