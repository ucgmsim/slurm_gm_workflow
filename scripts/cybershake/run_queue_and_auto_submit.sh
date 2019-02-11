#!/bin/bash

if [[ $# -lt 3 ]];then
    echo "please provide the path to the root folder of runs and sleep interval(seconds), and a config file that used to install the runs"
    echo "./run_queue_and_submit.sh /nesi/nobackup/nesi00213/RunFolder/Cybershake/v18p5/ 60 /path/to/cybershake/config_file"
    exit 1
fi

path_sim_root=$1
interval=$2
cybershake_cfg=$3
trap "echo Exited!; exit;" SIGINT SIGTERM

while [ 1 ];
do
    #run the commands queued up first, before submit
    while [[ `ls $path_sim_root/mgmt_db_queue` ]]
    do
        #if folder not exsist, create
        if [[ ! -d /tmp/cer ]];then
            echo "creating /tmp/cer/"
            mkdir /tmp/cer
            chmod 777 /tmp/cer
        fi
        cmd="$gmsim/workflow/scripts/cybershake/run_db_queue.sh $path_sim_root"
        echo $cmd
        $cmd
    done
    
    if [[ ! -d /tmp/cer ]];then
        echo "creating /tmp/cer/"
        mkdir /tmp/cer
        chmod 777 /tmp/cer/
    fi
    cmd="python $gmsim/workflow/scripts/cybershake/auto_submit.py $path_sim_root --config $cybershake_cfg --user `whoami`"
    echo $cmd
    $cmd
    sleep $interval
done
