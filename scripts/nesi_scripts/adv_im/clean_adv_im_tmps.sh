#!/bin/bash
if [[ $# -lt 2 ]];then
    echo please provide the run_dir, list of event
    exit 1
fi

root_dir=$1
run_dir=$root_dir/Runs
obs_dir=$root_dir/ObservedGroundMotions
event_list=$2

for event in `cat $event_list | awk '{print $1}'`;
do
    for sim_dir in $run_dir/$event/*/;
    do
        #echo $sim_dir
        im_dir=$sim_dir/IM_calc/
        for tmp_outfile in `find $im_dir -name "*.out"`;
        do
            #echo 
            rm $tmp_outfile
        done
    done

    #OBS
    if [[ -d $obs_dir/IM_calc ]];then
        for tmp_outfile in `find $obs_dir/IM_calc/$event/ -name "*.out"`;
        do
            #echo $tmp_outfile
            rm $tmp_outfile
        done    
    fi
done
