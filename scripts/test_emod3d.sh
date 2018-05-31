#!/bin/bash

#get run_name from $1

if [[ $# -lt 1 ]]; then
    echo "please provide the run name"
    exit 1
fi

#get model list

cd $1

list_model=`ls LF/`
run_name=`python -c "from params_base import *; print run_name"`

model_count=`echo $list_model | wc | awk '{print $2}'`
finished_model_count=$(expr 0)
for model in $list_model;
do
    #check Rlog
    cd $1/LF/$model/Rlog
    #abort rest of the code if cd returned not 0 (folder does not exist, emod3d has not yet been run)
    if [[ $? != 0 ]]; then
        echo "$model: have not yet run EMOD3D"
        continue
    fi
    rlog_count=$(expr 0) 
    for rlog in *;
    do
        rlog_count=`expr $rlog_count + 1`
        grep "IS FINISHED" $rlog >>/dev/null
        if [[ $? == 0 ]];
        then
            rlog_check=1
#            echo "rlog =1"
        else
            rlog_check=0
            echo "$rlog not finsihed"
            break
    
        fi
    done
    #echo $rlog_count
    #a AND check to see if the core number is the power of 2
    #if [[ $(( $rlog_count & (( $rlog_count - 1)) )) == 0 ]];then
    #    rlog_count_check=1
    rlog_count_check=1
    #    #echo "rlog_check 1"
    #else
    #    rlog_count_check=0
    #    echo "$model : rlog count seems to be wrong"
    #fi
    if [[ $rlog_check == 1 ]] && [[ $rlog_count_check == 1 ]];
    then
        echo "$model: EMOD3D completed"
        finished_model_count=`expr $finished_model_count + 1`
    else
        echo "$model: EMOD3D not completed"
    fi
    
done

#print out a special message if all models in a run is finished
if [[ $finished_model_count == $model_count ]]; then
    echo "===================="
    echo "$run_name finished"
    echo "===================="
fi


