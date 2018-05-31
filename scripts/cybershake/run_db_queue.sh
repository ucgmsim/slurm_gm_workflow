#!/bin/bash

if [[ $# -lt 1 ]];then
    echo "please provide the path root of runs"
    exit 1
fi

path_db_queue=$1/mgmt_db_queue
for f in $path_db_queue/*;
do
    while IFS= read -r cmd; do
        printf '%s\n' "$cmd"
        $cmd
    done < "$f"
done
