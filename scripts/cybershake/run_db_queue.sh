#!/bin/bash

if [[ $# -lt 1 ]];then
    echo "please provide the path root of runs"
    exit 1
fi
path_db_queue=$1/mgmt_db_queue

#test if there is files under queue folder
if [ -n "$(ls -A $path_db_queue 2>/dev/null)" ];
then

    for f in $path_db_queue/*;
    do
        cat $f
        bash $f
        if [[ $? == 0 ]]; then
            rm $f
        else
            echo "Error while executing $f"
            sleep 10
        fi
    done
else
    echo "no queue-ed commands to run"
fi
