#!/bin/bash

#get run_name from $1

if [[ $# -lt 1 ]]; then
    echo "please provide the run name"
    exit 1
fi
#add in workflow directory to pythonpath
#export PYTHONPATH=$PYTHONPATH:/nesi/projects/nesi00213/workflow/devel/
#assuming size of the node is 128
node_size=128
#get model list

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
    lf_count=`ls LF/$model/Vel | wc | awk '{print $1}'`
    #echo $lf_count 
    if [[ $lf_count == $fd_count ]]; 
    then
        check_file_count=1
    else
        check_file_count=0
        echo "$model: file count does not match the station number"
    fi
   
    #check all file size matches by calling functions in remaining_station.py
    #checK_file_size will be 0 if some files are not matched
    #this will only be run when the file count is matched with station number
    if [[ $check_file_count == 1 ]];
    then
        check_file_size=`python -c "from shared_workflow import remaining_stations as rmst; import sys,os; sys.stdout = open(os.devnull, 'w')  ;list=rmst.get_lines('$fd_ll','$1/LF/$model/Vel',$node_size); sys.stdout = sys.__stdout__; print('1' if not list else list)"`

        #echo $check_file_size
        if [[ $check_file_size == 0 ]];
        then
            echo "some station vel does not match in file size"
        fi
    fi
     
   
    #check all seis in outbin have a matching xyts file
    #TODO: this test is not valid, seis does not has to match the count of xyts
    #cd $1/LF/$model/OutBin
    #list_seis=`ls *seis* | cut -d- -f2`
    #for seis in $list_seis;
    #do
    #    ls $run_name\_xyts-$seis >/dev/null
    #    if [[ $? == 0 ]];
    #    then
    #        xyts_check=1
    #    else
    #        xyts_check=0
    #        echo "xyts for $seis is missing"
    #        break
    #    fi
    #done    
    xyts_check=1
    
    #check if $run_name_xyts.e3d is there
    cd $1
    ls LF/$model/OutBin/$run_name\_xyts.e3d >/dev/null
    if [[ $? != 0 ]];
    then
        xyts_check=0
        echo "$run_name\_xyts.e3d missing"
    fi
    
    if [[ $xyts_check == 1  ]] && [[ $check_file_size == 1 ]] && [[ $check_file_count == 1 ]];
    then
        echo "$model: post_emod3d completed"
        finished_model_count=`expr $finished_model_count + 1`
    else
        echo "$model: post_emod3d did not complete, read message above for info"
    fi

done

#print out a special message if all models in a run is finished
if [[ $finished_model_count == $model_count ]]; then
    echo "===================="
    echo "$run_name finished"
    echo "===================="
fi

