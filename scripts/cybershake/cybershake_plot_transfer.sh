#!/bin/bash
if [[ $# -lt 4 ]]; then
    echo "please provide a path to runs, and username"
    echo "cybershake_plot_transfer.sh /path/on/fitzroy/runs /path/on/hypo/runs list_vm username"
    echo "!!this script gets all the run_name under /path/to/hypo/runs"
    exit 1
fi
fitz_dir=$1
run_dir=$2
list_runs=`cat $3`
user_name=$4
script_location=`dirname $0`
for run_name in $list_runs ;
do
#    python $script_location/plot_transfer.py auto $fitz_dir/$run_name $run_dir/$run_name/GM/Sim/Data/ $user_name
    python $script_location/plot_transfer_local.py auto $fitz_dir/$run_name $run_dir/$run_name/GM/Sim/Data/ $user_name

done
