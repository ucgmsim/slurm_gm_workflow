#!/bin/bash

if [[ $# -lt 4 ]];then
    echo "please provide the path to the root folder of runs and sleep interval(seconds), and a config file that used to install the runs, and your hpc user name"
    echo "./run_queue_and_submit.sh /nesi/nobackup/nesi00213/RunFolder/Cybershake/v18p5/ 60 /path/to/cybershake/config_file melody.zhu"
    exit 1
fi

path_sim_root=$1
interval=$2
cybershake_cfg=$3
user=$4
trap "echo Exited!; exit;" SIGINT SIGTERM

while [ 1 ];
do
    #run the commands queued up first, before submit
    cmd='$gmsim/workflow/scripts/cybershake/run_db_queue.sh'" $path_sim_root"
    echo $cmd
    ssh maui "$cmd"

    cmd='python $gmsim/workflow/scripts/cybershake/auto_submit.py'" $path_sim_root --config $cybershake_cfg --no_im --user $user"
    echo $cmd
    ssh maui "source /nesi/project/nesi00213/test_multi_user/share/bashrc.uceq; load_python3_maui; $cmd"
    sleep $interval
done
