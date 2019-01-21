#!/bin/bash

if [[ $# -lt 4 ]];then
    echo "please provide the path to the root folder of runs and sleep interval(seconds), and a config file that used to install the runs. and a list of users"
    echo "./run_queue_and_submit.sh /nesi/nobackup/nesi00213/RunFolder/Cybershake/v18p5/ 60 /path/to/cybershake/config_file"
    exit 1
fi

path_sim_root=$1
interval=$2
cybershake_cfg=$3
trap "echo Exited!; exit;" SIGINT SIGTERM

#get all the usernames
user_list=${@:4}



while [ 1 ];
do
    #loop through users
    for user in $user_list;
    do
        #run update script until there is no files/commands in queued
        while [[ `ls $path_sim_root/mgmt_db_queue/*` ]]
        do
            cmd="source $gmsim/share/bashrc.uceq;$gmsim/workflow/scripts/cybershake/run_db_queue.sh $path_sim_root"
            echo $cmd
            ssh $user@maui "$cmd"
        done
        
        #run submit script
        cmd="source $gmsim/share/bashrc.uceq;python $gmsim/workflow/scripts/cybershake/auto_submit.py $path_sim_root --config $cybershake_cfg --user $user "
        echo $cmd
        ssh $user@maui "$cmd"
        sleep $interval
    done
done


#while [ 1 ];
#do
#
#    #run the commands queued up first, before submit
#    cmd="$gmsim/workflow/scripts/cybershake/run_db_queue.sh $path_sim_root"
#    echo $cmd
#    $cmd
#
#    cmd="python $gmsim/workflow/scripts/cybershake/auto_submit.py $path_sim_root --config $cybershake_cfg "
#    echo $cmd
#    $cmd
#    sleep $interval
#done
