#!/bin/bash

#get run_name from $1

if [[ $# -lt 2 ]]; then
    echo "please provide the path to sim_dir and srf_name"
    exit 1
fi

sim_dir=$1
srf_name=$2
run_name=`python -c "from qcore import utils; p = utils.load_yaml('$sim_dir/sim_params.yaml'); print(p['run_name'])"`
lf_sim_dir=$sim_dir/LF

cd $sim_dir

#check if $run_name_xyts.e3d is there
ls $lf_sim_dir/OutBin/$run_name\_xyts.e3d >/dev/null
if [[ $? != 0 ]];
then
    xyts_check=1
    echo "$run_name\_xyts.e3d missing"
    exit 1
else
    xyts_check=0
fi

python $gmsim/workflow/scripts/test_xyts.py $lf_sim_dir/OutBin/$run_name\_xyts.e3d
if [[ $? != 0 ]];
then
    xyts_test=1
    echo "$run_name\_xyts.e3d was not able to be loaded"
    exit 1
else
    xyts_test=0
fi

if [[ $xyts_check == 0  ]] && [[ $xyts_test == 0 ]];
then
    echo "$lf_sim_dir merge_ts completed"
    exit 0
else
    echo "$lf_sim_dir merge_ts did not complete, read message above for info"
    exit 1
fi


