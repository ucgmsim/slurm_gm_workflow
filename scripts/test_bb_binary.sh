#!/bin/bash

#get run_name from $1

if [[ $# -lt 3 ]]; then
    echo "please provide the sim_dir, bb_run_name, and srf_name"
    exit 1
fi

sim_dir=$1
bb_run_name=$2
srf_name=$3

cd $sim_dir
fd_ll=`python -c "from params_base import *; print FD_STATLIST"`

bb_sim_dir=`python -c "import os; print os.path.join('$sim_dir','BB')"`
bb_acc_dir=$bb_sim_dir/Acc
bb_bin=$bb_acc_dir/BB.bin

ls $bb_bin 2> /dev/null
if [[ $? != 0 ]]; then
    echo "$bb_sim_dir: have not yet run"
    exit 1
fi

#get the len of fd_ll
#fd_count=$(expr `cat $fd_ll | wc -l` \* 3)

#check the len(fd_ll) == len(hf.stations)
#check station names are not empty
python $gmsim/workflow/scripts/test_bb_binary.py $bb_bin $fd_ll

