#!/bin/bash

# link BB.bin from a previously completed runs
# $1 src_root_dir: the root directory of a previous automated simulation runs
# $2 des_root_dir: the root directory of a clean install of simultion runs
# $3 list_fault: a list to events to link

# IMPORTANT: structure is for validation runs only.
# TODO: translate into python and incorporate qcore.simluated_structure for flexibility


if [[ $# -lt 3 ]];then
    echo "please provide 1. source of observed data 2. destination root directory 3. list of events"
    exit 1
fi

src_dir=$1
des_dir=$2
list_r=$3

#check if obs director is there in des
if [[ ! -d $des_dir ]];then
    mkdir $des_dir
fi



for event in `cat $list_r | awk '{print $1}'`;
do
    echo $event
    ln -s $src_dir/$event $des_dir
done

mkdir $des_dir/IM_calc
