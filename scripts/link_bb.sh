#!/bin/bash
#IMPORTANT: this is used for validation run only

if [[ $# -lt 3 ]];then
    echo "pleave provide 1. source root_dir 2.destination root_dir 3. list of fault"
    exit 1
fi

src_root_dir=$1
des_root_dir=$2
list_fault=$3

for fault in `cat $list_fault | awk '{print $1}'`;
do
    echo $fault
    #mkdir make sure BB/Acc is there
    mkdir -p $des_root_dir/Runs/$fault/$fault/BB/Acc/

    ln -s $src_root_dir/Runs/$fault/$fault/BB/Acc/BB.bin $des_root_dir/Runs/$fault/$fault/BB/Acc/
    
    #add BB completed into the mgmt_db queue
    python $gmsim/workflow/scripts/cybershake/add_to_mgmt_queue.py $des_root_dir/mgmt_db_queue $fault BB completed
done

#run queue_monitor to update the DB
