#!/bin/bash

if [[ $# -lt 4 ]];then
    echo "please provide the path to cybershake folder, cybershake config, the nhm selection file and the GMSim version"
    echo "install_cybershake.sh /path/to/cybershake/root/ /path/to/cybershake/config /path/to/cybershake/nhm_selection_file GMSim_version"
    exit 1
fi

script_location=`dirname $0`

cybershake_root=$1

sim_root_dir=$cybershake_root/Runs
vm_root_dir=$cybershake_root/Data/VMs

cybershake_cfg=$2

#get list of VM
#cd $vm_root_dir
fault_list=`cat $3`

#GMSim version
version=$4

#each vm match with multiple srf
IFS=$'\n'
for line in $fault_list;
do
    fault=`echo $line | awk '{print $1}'`
    n_rel=`echo $line | awk '{print $2}'`
    python $script_location/install_cybershake_fault.py $cybershake_root $cybershake_cfg $fault --n_rel ${n_rel//[nkr]} --version $version
done

#list_source='ls 
#echo $sim_root_dir
