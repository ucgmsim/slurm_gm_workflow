#!/bin/bash

if [[ $# -lt 1 ]]; then
    echo "please provide the path to sim_dir"
    exit 1
fi

sim_dir=$1
lf_sim_dir=$sim_dir/LF

# check if $xyts.e3d file is there
xyts_file=$(ls $lf_sim_dir/OutBin/*_xyts.e3d)
if [[ $? != 0 ]];
then
    echo "No combined xyts file found in $lf_sim_dir/OutBin"
    exit 1
fi

python $gmsim/workflow/workflow/calculation/verification/test_xyts.py $xyts_file
if [[ $? != 0 ]];
then
    echo "$xyts_file test has failed"
    exit 1
fi

echo "$lf_sim_dir merge_ts completed"
exit 0


