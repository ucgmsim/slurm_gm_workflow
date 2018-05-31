#!/bin/bash

#get the path to Runs from args
if [[ $# -lt 2 ]];then
    echo "please provide a path to runs and the list_vm"
    exit 1
fi

cwd=$1
sim_list=`cat $2`
echo "submitting BB for:"
echo "$sim_list"
echo "=============================="
for sim in $sim_list;
do
    echo "submitting for $sim"
    echo "=============================="
    cd $cwd/$sim
    echo '1' | python $gmsim/workflow/scripts/submit_bb.py 
    echo "=============================="
done
