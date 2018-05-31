#!/bin/bash

#get the path to Runs from args
if [[ $# -lt 3 ]];then
    echo "please provide a path to runs and the list_vm, and the input for install_bb"
    exit 1
fi
script_location=`dirname $0`
cwd=$1
sim_list=`cat $2`
input_bb_install=$3
echo "installing BB for:"
echo "$sim_list"
echo "=============================="
for sim in $sim_list;
do
    echo "installing BB for $sim"
    echo "=============================="
    cd $cwd/$sim
    cat $input_bb_install | python $gmsim/workflow/scripts/install_bb.py
    echo "=============================="
done

