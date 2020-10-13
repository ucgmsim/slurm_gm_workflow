#!/bin/bash

if [[ $# -lt 3 ]];then
    echo "please provide 1. source of observed data 2. destination root directory 3. list of events"
    exit 1
fi

src_dir=$1
des_dir=$2/ObservedGroundMotions
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
