#!/bin/bash

#get path to runs and list from args
if [[ $# -lt 2  ]]; then
    echo "please provide the path to Runs folder and the list of simulations"
    echo "e.g. get_corehours.sh /nesi/projects/nesi00213/Runfolder/Cybershake/v17p10/Runs ~/tmp/list_sim.txt"
    exit 1
fi

runs_path=$1

#test if the path provided is present
cd $runs_path
if [[ $? != 0 ]]; then
    exit 1
fi

#check if the list can be accessed with 'cat'
list_sim_path=$2
list_sim=`cat $list_sim_path`
if [[ $? != 0 ]]; then
    exit 1
fi

#check if bc is installed on the system, its required to do the calculation
echo '1+1' | bc 1>> /dev/null
if [[ $? != 0 ]]; then
    exit 1
fi

get_corehours_used(){
#this funciton expects 2 args, $1 is the ll_file, $2 is the $total_time
    ll_file=$1
    
    wall_clock_limit_txt=`grep 'wall_clock_limit' $ll_file | awk '{print $5}'`
    node_used_txt=`grep '@ node =' $ll_file | awk '{print $5}'`
    tasks_per_node_txt=`grep 'tasks_per_node' $ll_file | awk '{print $5}'`    
    
    

    #store the data in a array easier
    counter=`echo 0 | bc`
    for i in $wall_clock_limit_txt;
    do
        time_used_array[$counter]=$i
        counter=`echo $counter+1 | bc`
    done
    counter=`echo 0 | bc`
    for i in $node_used_txt;
    do
        node_used_array[$counter]=$i
        counter=`echo $counter+1 | bc`
    done

    counter=`echo 0 | bc`
    for i in $tasks_per_node_txt;
    do
        tasks_per_node_array[$counter]=$i
        counter=`echo $counter+1 | bc`
    done

    

    counter=`echo 0 | bc`
    for time_used in ${time_used_array[@]};
    do
        hours=`echo $time_used | cut -d: -f1`
        minutes=`echo $time_used | cut -d: -f2`
        seconds=`echo $time_used | cut -d: -f3`
        total_hours=`echo $total_time | cut -d: -f1`
        total_minutes=`echo $total_time | cut -d: -f2`
        total_seconds=`echo $total_time | cut -d: -f3`
        #get the line that contains '@ node =' and tasks_per_node
        #node_used=`grep '@ node =' $ll_file | awk '{print $5}'`
        #tasks_per_node=`grep 'tasks_per_node' $ll_file | awk '{print $5}'`
        node_used=${node_used_array[$counter]}
        tasks_per_node=${tasks_per_node_array[$counter]}
        core_used=`echo $node_used*$tasks_per_node | bc `

        #multiply the hours by the total core used

        hours=`echo $hours*$core_used | bc`
        minutes=`echo $minutes*$core_used | bc`
        seconds=`echo $seconds*$core_used | bc`
        
        #adding the core-hours used to the total used 

        total_hours=`echo $total_hours+$hours | bc`
        total_minutes=`echo $total_minutes+$minutes | bc`
        total_seconds=`echo $total_seconds+$seconds | bc`    
        #removing the floating points of seconds
        total_seconds=`echo $total_seconds | cut -d. -f1 `
        #roudning seconds
        if (( $total_seconds > 60 ));then
            total_minutes=`echo \($total_seconds/60\) +$total_minutes | bc`
            total_seconds=`echo $total_seconds%60 | bc`
        fi
        #rounding minutes
        if (( $total_minutes > 60 ));then
            total_hours=`echo \($total_minutes/60\) +$total_hours| bc`
            total_minutes=`echo $total_minutes%60 | bc`
        fi

        total_time=$total_hours:$total_minutes:$total_seconds
        counter=`echo $counter+1 | bc`
    done
}

#initializing the total var to store total time used
total_time='00:00:00'

for sim in $list_sim;
do
    echo running for $sim
    #get ll related to emod3d
    for ll_file in `ls $runs_path/$sim/run_bb_mpi_$sim_*.ll`;
    do
        #get the line that contains wall_clock_limit
        get_corehours_used $ll_file
    done
done

echo $total_time
