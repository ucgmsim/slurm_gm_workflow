#!/bin/bash

#get run_name from $1

if [[ $# -lt 3 ]]; then
    echo "please provide the sim_dir, hf_run_name, and srf_name"
    exit 1
fi

#this variable is used to check remaining station functions. the acutal number shoud not matter, as long as it is larger than 1.
node_size=80

sim_dir=$1
hf_run_name=$2
srf_name=$3


run_name=`python -c "from params_base import *; print run_name"`
fd_ll=`python -c "from params_base import *; print FD_STATLIST"`

bb_sim_dir=`python -c "import os; print os.path.join(os.path.join(os.path.join('$sim_dir','BB'), '$hf_run_name'), '$srf_name')"`
bb_acc_dir=$bb_sim_dir/Acc
bb_vel_dir=$bb_sim_dir/Vel

cd $bb_acc_dir 2> /dev/null
if [[ $? != 0 ]]; then
    echo "$bb_acc_dir: have not yet run"
    exit 1
fi

cd $bb_vel_dir 2> /dev/null
if [[ $? != 0 ]]; then
    echo "$bb_vel_dir: have not yet run"
    exit 1
fi


    #check for vel matches fd count
    fd_count=$(expr `cat $fd_ll | wc | awk '{print $1}'` \* 3)

    #Check Acc
    bb_acc_count=`ls $bb_acc_dir | wc | awk '{print $1}'`
    if [[ $bb_acc_count == $fd_count ]]; 
    then
        check_acc_file_count=0
        #echo "$model: BB finished"
    else
        check_acc_file_count=1
        #echo "$model: BB not finished"
        echo "$bb_acc_count: file count does not match the station number"
        exit 1
    fi

    #check Vel
    bb_vel_count=`ls $bb_vel_dir | wc | awk '{print $1}'`
    if [[ $bb_vel_count == $fd_count ]]; 
    then
        check_vel_file_count=0
        #echo "$model: BB finished"
    else
        check_vel_file_count=1
        #echo "$model: BB not finished"
        echo "$bb_acc_count: file count does not match the station number"
        exit 1
    fi

   
    #check file size
    #will only run when the file count matches station number
    if [[ $check_acc_file_count == 0 ]] && [[ $check_acc_file_count == 0 ]];
    then
        check_acc_file_size=`python -c "from shared_workflow import remaining_stations as rmst; import sys,os; sys.stdout = open(os.devnull, 'w'); list=rmst.get_lines('$fd_ll','$bb_acc_dir', $node_size); sys.stdout = sys.__stdout__; print('0' if not list else '1')"`
        if [[ $check_acc_file_size != 0 ]];
        then
            echo "$bb_sim_dir: some station Acc does not match in file size"
            exit 1
        fi
        check_vel_file_size=`python -c "from shared_workflow import remaining_stations as rmst; import sys,os; sys.stdout = open(os.devnull, 'w'); list=rmst.get_lines('$fd_ll','$bb_vel_dir', $node_size); sys.stdout = sys.__stdout__; print('0' if not list else '1')"`
        if [[ $check_vel_file_size != 0 ]];
        then
            echo "$bb_sim_dir: some station Vel does not match in file size"
            exit 1
        fi

    fi
    if [[ $check_acc_file_size == 0 ]] && [[ $check_vel_file_size == 0 ]] && [[ $check_acc_file_count == 0 ]] && [[ $check_vel_file_count == 0 ]];
    then
        echo "$bb_sim_dir: BB completed"
        exit 0
    else
        echo "$bb_sim_dir: BB did not complete, read message above for info"
        exit 1
    fi




