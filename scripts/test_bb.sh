#!/bin/bash

#get run_name from $1

if [[ $# -lt 1 ]]; then
    echo "please provide the run name"
    exit 1
fi

#add in workflow directory to pythonpath
#export PYTHONPATH=$PYTHONPATH:/nesi/projects/nesi00213/workflow/devel/
node_size=80
#get model list
#probably want to get this from params_base instead, when the structur changed
cd $1

run_name=`python -c "from params_base import *; print run_name"`
fd_ll=`python -c "from params_base import *; print FD_STATLIST"`

list_model=`ls LF/`

model_count=`echo $list_model | wc | awk '{print $2}'`
finished_model_count=$(expr 0)

for model in $list_model;
do
    #check for vel matches fd count
    fd_count=$(expr `cat $fd_ll | wc | awk '{print $1}'` \* 3)
    #echo "fd count: $fd_count"
    bb_count=`ls BB/Cant1D_v2-midQ_leer_hfnp2mm+_rvf0p8_sd50_k0p045/$model/Acc | wc | awk '{print $1}'`
    #echo $bb_count 
    if [[ $bb_count == $fd_count ]]; 
    then
        check_file_count=1
        #echo "$model: BB finished"
    else
        check_file_count=0
        #echo "$model: BB not finished"
        echo "$model: file count does not match the station number"
    fi
   
    #check file size
    #will only run when the file count matches station number
    if [[ $check_file_count == 1 ]];
    then
        check_file_size=`python -c "from shared_workflow import remaining_stations as rmst; import sys,os; sys.stdout = open(os.devnull, 'w'); list=rmst.get_lines('$fd_ll','$1/BB/Cant1D_v2-midQ_leer_hfnp2mm+_rvf0p8_sd50_k0p045/$model/Vel', $node_size); sys.stdout = sys.__stdout__; print('1' if not list else list)"`
        if [[ $check_file_size == 0 ]];
        then
            echo "some station vel does not match in file size"
        fi
    fi
    if [[ $check_file_size == 1 ]] && [[ $check_file_count == 1 ]];
    then
        echo "$model: BB completed"
        finished_model_count=`expr $finished_model_count + 1`
    else
        echo "$model: BB did not complete, read message above for info"
    fi



done

#print out a special message if all models in a run is finished
if [[ $finished_model_count == $model_count ]]; then
    echo "===================="
    echo "$run_name finished"
    echo "===================="
fi

