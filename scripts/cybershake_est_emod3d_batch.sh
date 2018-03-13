#!/bin/bash



if [[ $# -lt 3 ]];then
    echo "Usage: cybershake_est_emod3d.sh /path/to/the/VM/ /path/to/rupi batch_list"
    exit 1
fi

script_path=`realpath $0 | xargs dirname`
path_vm=$1
path_srf=$2
file_batch=$3

list_batch=`cat $file_batch`
printf "\r %15s %5s %5s %6s %6s %6s %5s %12s %20s %20s\n" 'rup_name' 'hypo' 'slip' 'nx' 'ny' 'nz' 'dt' 'sim_duration' 'core_hours' 'core_hours_total'

ch_whole_version=`echo 0 | bc`
for list_file in $list_batch;
do
    list_sim=`cat $list_file`
    ch_batch_total=`echo 0 | bc`

    for rup in $list_sim;
    do
        result=$($script_path/cybershake_est_emod3d.sh $rup /home/vap30/scratch/karim86_vm/$rup/ /home/vap30/scratch/karim86_srf/$rup/Srf)
        #printf "\r %15s %5s %5s %6s %6s %6s %5s %12s %20s %20s\n" $rup_name $count_hypo $count_slip $nx $ny $nz $dt $sim_duration $estimated_core_hours $estimated_core_hours_total
        echo "$result"
        rup_total_ch=`echo $result| awk '{print $11}'`
        #echo $rup_total_ch
        ch_batch_total=`echo $ch_batch_total+$rup_total_ch | bc`
    done
    echo "************************"
    echo "total core hour for batch: $ch_batch_total"
    echo "************************"

    ch_whole_version=`echo $ch_whole_version+$ch_batch_total | bc`
done

echo "total time for whole version: $ch_whole_version"
