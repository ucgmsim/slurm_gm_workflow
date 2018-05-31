#!/bin/bash

#take 1st arg as the runfolder directory
#take 2nd arg as the list to test

if [[ $# -lt 2 ]];then
    echo "please provide the path to run folder and the list_vm"
    echo "test_cybershake_hf.sh /path/to/runs/ list_vm"
    exit 1
fi

list_runs=`cat $2`
run_dir=$1

#get the current script location and call the test_emod3d.sh script from a layer above
script_location=`dirname $0`

for run in $list_runs;
do
    echo "running test for $run"
    $script_location/../test_hf.sh $run_dir/$run
done
