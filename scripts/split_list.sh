#!/bin/bash

if [[ $# -lt 3 ]];then
    echo "please provide 1. list of events 2. batch size 3. destination of temporary location to store the list" 
    exit 1
fi

list_r=$1
batch_size=$2
des_dir=$3

list_fname=`basename $list_r`

# -a : the length of suffix
# -d : in digital instead of alphabetical(default)
# -l : the number of lines (events in our case)

split -a 2 -d -l $batch_size $list_r $des_dir/$list_fname\_
