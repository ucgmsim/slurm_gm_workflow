#!/bin/bash

#get run_name from $1

if [[ $# -lt 1 ]]; then
    echo "please provide the run name"
    exit 1
fi

#add in workflow directory to pythonpath
export PYTHONPATH=$PYTHONPATH:/nesi00213/workflow/devel/
node_size=80
#get model list

run_dir=$1
cd $run_dir

run_name=`python -c "from params_base import *; print run_name"`
fd_ll_dir=`python -c "from params_base import *; print FD_STATLIST"`
fd_ll=`basename $fd_ll_dir`

#get the model list from LF
#probably want to get this from params_base instead, when the structur changed
list_model=`ls LF/`

model_count=`echo $list_model | wc | awk '{print $2}'`
finished_model_count=$(expr 0)

for model in $list_model;
do
    #check for Acc matches fd count
    #get station count
    fd_count=$(expr `cat $fd_ll | wc | awk '{print $1}'` \* 3)
    #echo "fd count: $fd_count"
    hf_count=`ls HF/Cant1D_v2-midQ_leer_hfnp2mm+_rvf0p8_sd50_k0p045/$model/Acc | wc | awk '{print $1}'`
    #echo $hf_count 
    if [[ $hf_count == $fd_count ]]; 
    then
        check_file_count=1
        #echo "$model: HF finished"
    else
        check_file_count=0
        #echo "$model: HF not finished"
        echo "$model: file count does not match the station number"
    fi
    
    #check file size 
    #will only run when the file count matches station number
    if [[ $check_file_count == 1 ]];
    then
        check_file_size=`python -c "from shared_workflow import remaining_stations as rmst; import sys,os; sys.stdout = open(os.devnull, 'w'); list=rmst.get_lines('$fd_ll','$1/HF/Cant1D_v2-midQ_leer_hfnp2mm+_rvf0p8_sd50_k0p045/$model/Acc', $node_size); sys.stdout = sys.__stdout__; print('1' if not list else '0')"`
        if [[ $check_file_size == 0 ]];
        then
            echo "some station files does not match in file size"
        fi
    fi
    if [[ $check_file_size == 1 ]] && [[ $check_file_count == 1 ]];
    then
        echo "$model: HF completed"
        finished_model_count=`expr $finished_model_count + 1`
    else
        echo "$model: HF did not complete, read message above for info"
    fi  
done

#print out a special message if all models in a run is finished
if [[ $finished_model_count == $model_count ]]; then
    echo "===================="
    echo "$run_name finished"
    echo "===================="
fi

