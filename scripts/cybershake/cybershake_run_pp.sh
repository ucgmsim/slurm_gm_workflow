#!/bin/bash
if [[ $# -lt 2 ]]; then
    echo "please provide a path to runs and list"
    exit 1
fi

run_dir=$1
list_runs=`cat $2`

#read the completed pp list
completed_pp_list=`cat $run_dir/../pp_completed.log`
date=`date +%Y%m%d_%H%M`
for run_name in $list_runs ;
do
    #check if run_name is contained in the completed_pp_list;
    #run plot if not contained in completed_pp_list, otherwise continue the loop for next run_name
    [[ $completed_pp_list =~ (^|[[:space:]])$run_name($|[[:space:]]) ]] && continue
    for i in `find $run_dir/$run_name -name "pp_config.cfg"`;
    do
        echo "python -u /home/nesi00213/post-processing/examples/run_simulation_plots.py $i 2>>$i.$date.log"
        python -u /home/nesi00213/dev/post-processing/examples/run_default_plots.py $i 2>>$i.$date.log
    done
    echo "$run_name" >> $run_dir/../pp_completed.log
done
