#!/bin/bash

##quick script to wrap around est_emod3d to get all the estimated core hours for cybershake
#echo $#
#TODO: get the slip number from reading NHM or ls from generated SRF module files
#      may need re-factor of the whole code
count_slip=3
#TODO: make this more flexable
dt=0.005

if [[ $# -lt 3 ]];then
    echo "Usage: cybershake_est_emod3d.sh rup_name /path/to/the/VM/params /path/to/rup/srf"
    exit 1
fi

script_path=`realpath $0 | xargs dirname`
rup_name=$1
path_vm=$2
path_srf=$3

#get the number of srfs
count_srf=`ls $path_srf/*.srf | wc | awk '{print $1}'`
#echo $list_srf

#divide the count_srf by count_slip to get the count_hypo
count_hypo=`echo $count_srf/$count_slip | bc`
#echo $count_hypo


cd $path_vm

params=`python -c"from qcore.utils import load_yaml; d=load_yaml('params_vel.yaml'); print(d['nx'],d['ny'],d['nz'],d['hh'],d['sim_duration'])"`

params_array=($params)
nx=${params_array[0]}
ny=${params_array[1]}
nz=${params_array[2]}
hh=${params_array[3]}
sim_duration=${params_array[4]}

cd $script_path
estimated_core_hours=`python -c "import estimate_emod3d as est; print est.est_core_hours_emod3d($nx,$ny,$nz,$dt,$sim_duration)"`
estimated_core_hours_total=`echo $estimated_core_hours*$count_srf | bc`

#printf "\r %15s %5s %5s %6s %6s %6s %5s %12s %20s %20s\n" 'rup_name' 'hypo' 'slip' 'nx' 'ny' 'nz' 'dt' 'sim_duration' 'core_hours' 'core_hours_total'
printf "\r %15s %5s %5s %6s %6s %6s %5s %12s %20s %20s\n" $rup_name $count_hypo $count_slip $nx $ny $nz $dt $sim_duration $estimated_core_hours $estimated_core_hours_total
