#!/bin/bash

#get run_name from $1

if [[ $# -lt 3 ]]; then
    echo "please provide the sim_dir, hf_run_name, and srf_name"
    exit 1
fi

#add in workflow directory to pythonpath
#this variable is used to check remaining station functions. the acutal number shoud not matter, as long as it is larger than 1.
node_size=80

sim_dir=$1
hf_run_name=$2
srf_name=$3


run_name=`python -c "from params_base import *; print(run_name)"`
fd_ll_dir=`python -c "from params_base import *; print(FD_STATLIST)"`
fd_ll=`basename $fd_ll_dir`
hf_sim_dir=`python -c "import os; print(os.path.join('$sim_dir','HF', '$hf_run_name', '$srf_name'))"`
hf_acc_dir=$hf_sim_dir/Acc

cd $hf_acc_dir 2> /dev/null
if [[ $? != 0 ]]; then
    echo "$hf_sim_dir: have not yet run"
    exit 1
fi

cd $sim_dir
#check for Acc matches fd count
#get station count
fd_count=$(expr `cat $fd_ll | wc | awk '{print $1}'` \* 3)
#echo "fd count: $fd_count"
hf_acc_count=`ls $hf_acc_dir | wc | awk '{print $1}'`
    
if [[ $hf_acc_count == $fd_count ]]; 
then
    check_file_count=0
    #echo "$model: HF finished"
else
    check_file_count=1
    echo "$hf_sim_dir: file count does not match the station number"
    exit 1
fi
    
#check file size 
#will only run when the file count matches station number
if [[ $check_file_count == 0 ]];
then
    check_file_size=`python -c "from shared_workflow import remaining_stations as rmst; import sys,os; sys.stdout = open(os.devnull, 'w'); list=rmst.get_lines('$fd_ll','$hf_acc_dir', $node_size); sys.stdout = sys.__stdout__; print('0' if not list else '1')"`
    if [[ $check_file_size != 0 ]];
    then
        echo "$hf_sim_dir: some station files does not match in file size"
        exit 1
    fi
fi
if [[ $check_file_size == 0 ]] && [[ $check_file_count == 0 ]];
then
    echo "$hf_sim_dir: HF completed"
    exit 0
else
    echo "$hf_sim_dir: HF did not complete, read message above for info"
    exit 1
fi  



