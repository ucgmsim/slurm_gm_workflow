#!/bin/bash

#get run_name from $1

if [[ $# -lt 1 ]]; then
    echo "please provide the sim_dir, and srf_name"
    exit 1
fi

sim_dir=$1
#hf_run_name=$2
#srf_name=$3

cd $sim_dir
fd_ll=`python -c "from workflow.automation import sim_params; p = sim_params.load_sim_params('sim_params.yaml'); print(p['FD_STATLIST'])"`

hf_sim_dir=$sim_dir/HF
hf_acc_dir=$hf_sim_dir/Acc
hf_bin=$hf_acc_dir/HF.bin

ls $hf_bin 2> /dev/null
if [[ $? != 0 ]]; then
    echo "$hf_sim_dir: have not yet run"
    exit 1
fi

#get the len of fd_ll
#fd_count=$(expr `cat $fd_ll | wc -l` \* 3)

#check the len(fd_ll) == len(hf.stations)
#check station names are not empty
echo $fd_ll
python $gmsim/workflow/workflow/calculation/verification/test_binary.py $hf_bin $fd_ll hf --verbose

