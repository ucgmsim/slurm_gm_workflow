#!/bin/bash

#get run_name from $1

if [[ $# -lt 2 ]]; then
    echo "please provide the path to sim_dir and srf_name"    
    exit 1
fi
#this variable is used to check remaining station functions. the acutal number shoud not matter, as long as it is larger than 1.
node_size=80

sim_dir=$1
srf_name=$2
run_name=`python -c "from params_base import *; print(run_name)"`
lf_sim_dir=`python -c "import os; print(os.path.join('$sim_dir','LF', '$srf_name'))"`
lf_vel_dir=$lf_sim_dir/Vel

cd $lf_vel_dir 2> /dev/null
if [[ $? != 0 ]]; then
    echo "$lf_sim_dir: have not yet run winbin_aio"
    exit 1
fi


cd $sim_dir

run_name=`python -c "from params_base import *; print(run_name)"`
fd_ll=`python -c "from params_base import *; print(FD_STATLIST)"`

#check for vel matches fd count
fd_count=$(expr `cat $fd_ll | wc | awk '{print $1}'` \* 3)
#echo "fd count: $fd_count"
lf_vel_count=`ls $lf_vel_dir | wc | awk '{print $1}'`

if [[ $lf_vel_count == $fd_count ]]; 
then
    check_file_count=0
else
    check_file_count=1
    echo "$lf_sim_dir: file count does not match the station number"
    exit 1
fi

#passed count check
   
#check all file size matches by calling functions in remaining_station.py
#checK_file_size will be !0 if some files are not matched
#this will only be run when the file count is matched with station number
if [[ $check_file_count == 0 ]];
then
    check_file_size=`python -c "from shared_workflow import remaining_stations as rmst; import sys,os; sys.stdout = open(os.devnull, 'w')  ; list=rmst.get_lines('$fd_ll','$lf_vel_dir',$node_size); sys.stdout = sys.__stdout__; print('0' if not list else '1')"`

    #echo $check_file_size
    if [[ $check_file_size != 0 ]];
    then
        echo "some station vel does not match in file size"
        exit 1
    fi
fi
     
   
    
if [[ $check_file_size == 0 ]] && [[ $check_file_count == 0 ]];
then
    echo "$lf_sim_dir winbin_aio completed"
    exit 0
else
    echo "$lf_sim_dir winbin_aio did not complete, read message above for info"
    exit 1
fi

