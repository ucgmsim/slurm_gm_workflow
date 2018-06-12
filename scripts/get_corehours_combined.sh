#!/bin/bash

#get path to runs and list from args
if [[ $# -lt 3  ]]; then
    echo "please provide the path to Runs folder and the list of simulations"
    echo "e.g. get_corehours.sh /nesi/projects/nesi00213/Runfolder/Cybershake/v17p10/Runs ~/tmp/list_sim.txt ~/tmp/logs/"
    exit 1
fi

runs_path=$1
logs_path=$3
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

scan_o_file_hf_bb(){
    o_file_name=$1
    pattern='UTC'
#    sim=$2
    hypo_name=$2
    
    counter_ll_num=`echo 0 | bc`
    if [[ $o_file_name == *"run_hf_mpi"*  ]];then
        for ll_file in `ls $runs_path/$sim/run_hf_mpi*$hypo_name*.sl`;
        do
            ll_file_array[$counter_ll_num]=$ll_file
            counter_ll_num=`echo $counter_ll_num +1 | bc`
        done
    else
        for ll_file in `ls $runs_path/$sim/run_bb_mpi*$hypo_name*.sl`;
        do
            ll_file_array[$counter_ll_num]=$ll_file
            counter_ll_num=`echo $counter_ll_num +1 | bc`
        done
    fi

    counter_used_time=`echo 0 | bc`
    for wct_used in `grep "$pattern" $o_file_name | awk '{print $4}' `;
    do
        hf_time_array[$counter_used_time]=$wct_used
        counter_used_time=`echo $counter_used_time+1 | bc`
    done

    if (( $counter_used_time > 1 ));then
        #get the time diff
        start_hour=`echo ${hf_time_array[0]} | cut -d: -f1| bc`
        start_minute=`echo ${hf_time_array[0]} | cut -d: -f2 | bc`
        start_second=`echo ${hf_time_array[0]} | cut -d: -f3 | bc`
        end_hour=`echo ${hf_time_array[1]} | cut -d: -f1 | bc`
        end_minute=`echo ${hf_time_array[1]} | cut -d: -f2 | bc`
        end_second=`echo ${hf_time_array[1]} | cut -d: -f3 | bc`

#        echo $end_hour:$end_minute:$end_second
#        echo $start_hour:$start_minute:$start_second
        #echo ${hf_time_array[0]}
        #echo ${hf_time_array[1]}
        if (( $end_hour >= $start_hour ));then
            #echo "larger or equal"
            hour=` echo $end_hour - $start_hour | bc`
        else
            #echo "smaller"
            hour=` echo $end_hour+24 - $start_hour | bc`
        fi


        if (( $end_minute >= $start_minute ));then
            minute=`echo $end_minute - $start_minute | bc`
        else
            minute=`echo $end_minute+60  - $start_minute | bc`
            hour=` echo $hour -1 | bc`
        fi

 
        if (( $end_second >= $start_second ));then
            second=`echo $end_second - $start_second | bc`
        else
            second=`echo $end_second+60 - $start_second | bc`
            minute=`echo $minute -1 | bc`
        fi

        if (( $minute < 0 ));then
            minute=` echo $minute + 60 | bc`
            hour=` echo $hour -1 | bc `
        fi
        if (( $hour < 0 ));then
            hour=` echo 0 | bc `
        fi

#        echo $o_file_name
        time_used=$hour:$minute:$second
    else 
        #get the time used in the ll
        time_used=`grep 'wall_clock_limit' ${ll_file_array[0]} | awk '{print $5}'`
    fi
    get_corehours_used ${ll_file_array[0]} $time_used 0
}




scan_o_file_emod3d(){
    #scan through a particulare file to find a start and end time, return -1 if cannot find a end time

    o_file_name=$1
    pattern='Wall clock time used'
    sequence=$2 #the sqeunce in the list of .lls
    hypo_name=$3
    #a=`grep "$pattern" $o_file_name | awk '{print $5}' ` 
    #echo "grep '$pattern' $o_file_name | awk '{print \$5}'"

    #store the list of ll files in a array
    counter=`echo 0 | bc`
    for ll_file in `ls $runs_path/$sim/run_emod3d_$sim\_$hypo_name*.sl`;
    do
#        echo $ll_file
        ll_file_array[$counter]=$ll_file
        counter=`echo $counter+1 | bc`
    done
    #search for patter in the o_file_name
    counter=`echo 0 | bc`
#    echo $o_file_name
    for wct_used in `grep "$pattern" $o_file_name | awk '{print $5}' `;
    do
        #echo $utc_txt
        #log
        #scan .ll script to ge the nodesize
 #       echo $wct_used
 #       echo get_corehours_used ${ll_file_array[$counter]} $wct_used 0
        get_corehours_used ${ll_file_array[$counter]} $wct_used 0 
        counter=`echo $counter+1 | bc`
    done
}

scan_o_file_post_emod3d(){
    o_file_name=$1
    #the pattern if post_emod3d .o is bad, need to fix this
    pattern='UTC 2018'
    sequence=$2 #the sqeunce in the list of .lls
    hypo_name=$3 
    counter=`echo 0 | bc`
    
    for ll_file in `ls $runs_path/$sim/post_emod3d_winbin_aio_$hypo_name*.sl`;
    do
        ll_file_array[$counter]=$ll_file
        counter=`echo $counter+1 | bc` 
    done
    #search for patter in the o_file_name
    counter_used_time=`echo 0 | bc`
#    echo $o_file_name 
    for pp_time in ` grep "$pattern" $o_file_name | awk '{print $4}' `
    do
        pp_time_array[$counter_used_time]=$pp_time
        counter_used_time=`echo $counter_used_time+1 | bc`
    done
    
    if (( $counter_used_time > 1 ));then
        #get the time diff
        start_hour=`echo ${pp_time_array[0]} | cut -d: -f1| bc`
        start_minute=`echo ${pp_time_array[0]} | cut -d: -f2 | bc`
        start_second=`echo ${pp_time_array[0]} | cut -d: -f3 | bc`
        end_hour=`echo ${pp_time_array[1]} | cut -d: -f1 | bc`
        end_minute=`echo ${pp_time_array[1]} | cut -d: -f2 | bc`
        end_second=`echo ${pp_time_array[1]} | cut -d: -f3 | bc`


#        echo $end_hour:$end_minute:$end_second
#        echo $start_hour:$start_minute:$start_second
        if (( $end_hour >= $start_hour ));then
            #echo "larger or equal"
            hour=` echo $end_hour - $start_hour | bc`
        else
            #echo "smaller"
            hour=` echo $end_hour+24 - $start_hour | bc`
        fi


        if (( $end_minute >= $start_minute ));then
            minute=`echo $end_minute - $start_minute | bc`
        else
            minute=`echo $end_minute+60  - $start_minute | bc`
            hour=` echo $hour -1 | bc`
        fi
        

        if (( $end_second >= $start_second ));then
            second=`echo $end_second - $start_second | bc`
        else
            second=`echo $end_second+60 - $start_second | bc`
            minute=`echo $minute -1 | bc`
        fi

        if (( $minute < 0 ));then
            minute=` echo $minute + 60 | bc`
            hour=` echo $hour -1 | bc `
        fi
        if (( $hour < 0 ));then
            hour=` echo 0 | bc `
        fi

#        echo $o_file_name
        time_used=$hour:$minute:$second
    else 
        if [[ $o_file_name == *"winbin_aio"*  ]];then
            echo "winbin_aio did not finished with on submition" >> $logs_path/$sim.log
            time_used=02:00:00
        else
            echo "merg_ts did not finish in one submition" >> $logs_path/$sim.log
            time_used=0:30:00
        fi
    fi
    if [[ $o_file_name == *"winbin_aio"*  ]];then
        echo "winbin_aio" >> $logs_path/$sim.log
        get_corehours_used ${ll_file_array[0]} $time_used 0
    else
        echo "merg_ts" >> $logs_path/$sim.log
        get_corehours_used ${ll_file_array[0]} $time_used 0
    fi
}

get_corehours_used(){
#this funciton expects 2 args, $1 is the ll_file, $2 patter to find 
    ll_file=$1
    echo "ll_file: $ll_file " >> $logs_path/$sim.log 
    wall_clock_limit_txt=$2
    echo "wct: $wall_clock_limit_txt" >> $logs_path/$sim.log
#    echo $1 $2
    #node_used_txt=`grep '@ node =' $ll_file | awk '{print $5}'`
    #this not used in slurm script anymore
    node_used_txt=1
    echo "nodes: $node_used_txt " >> $logs_path/$sim.log
    #old
    #tasks_per_node_txt=`grep 'tasks_per_node' $ll_file | awk '{print $5}'`    
    tasks_per_node_txt=`grep 'ntasks' $ll_file | cut -d= -f2`    
    echo "task_per_node: $tasks_per_node_txt" >> $logs_path/$sim.log

    time_per_hypo="0:0:0"

    
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

    
    counter=$3
#    echo $counter
    for time_used in ${time_used_array[@]};
    do
        hours=`echo $time_used | cut -d: -f1`
        minutes=`echo $time_used | cut -d: -f2`
        seconds=`echo $time_used | cut -d: -f3`

#        echo $sim_total_time
        sim_total_hours=`echo $sim_total_time | cut -d: -f1`
        sim_total_minutes=`echo $sim_total_time | cut -d: -f2`
        sim_total_seconds=`echo $sim_total_time | cut -d: -f3`

#        sim_total_hours_sum=`echo $sim_total_time_sum | cut -d: -f1`
#        sim_total_minutes_sum=`echo $sim_total_time_sum | cut -d: -f2`
#        sim_total_seconds_sum=`echo $sim_total_time_sum | cut -d: -f3`
        
#        total_hours=`echo $total_time | cut -d: -f1`
#        total_minutes=`echo $total_time | cut -d: -f2`
#        total_seconds=`echo $total_time | cut -d: -f3`

        #get the line that contains '@ node =' and tasks_per_node
        #node_used=`grep '@ node =' $ll_file | awk '{print $5}'`
        #tasks_per_node=`grep 'tasks_per_node' $ll_file | awk '{print $5}'`
        node_used=${node_used_array[$counter]}
        tasks_per_node=${tasks_per_node_array[$counter]}
        core_used=`echo $node_used*$tasks_per_node | bc `
#        echo core_used=$core_used
        #multiply the hours by the total core used
        hours=`echo $hours*$core_used | bc`
        minutes=`echo $minutes*$core_used | bc`
        seconds=`echo $seconds*$core_used | bc`
#        echo multiplied=$hours:$minutes:$seconds 
        sim_total_hours=`echo $sim_total_hours+$hours | bc`
        sim_total_minutes=`echo $sim_total_minutes+$minutes | bc`
        sim_total_seconds=`echo $sim_total_seconds+$seconds | bc`
        sim_total_seconds=`echo $sim_total_seconds | cut -d. -f1`
#        echo simm_total_*=$sim_total_hours:$sim_total_minutes:$sim_total_seconds
#        sim_total_hours_sum=`echo $sim_total_hours_sum+$hours | bc`
#        sim_total_minutes_sum=`echo $sim_total_minutes_sum+$minutes | bc`
#        sim_total_seconds_sum=`echo $sim_total_seconds_sum+$seconds | bc`
#        sim_total_seconds_sum=`echo $sim_total_seconds_sum | cut -d. -f1`
        #adding the core-hours used to the total used 
#        total_hours=`echo $total_hours+$hours | bc`
#        total_minutes=`echo $total_minutes+$minutes | bc`
#        total_seconds=`echo $total_seconds+$seconds | bc`    
        

        time_per_hypo_hours=$hours 
        time_per_hypo_minutes=$minutes
        time_per_hypo_seconds=$seconds
        #removing the floating points of seconds
 #       total_seconds=`echo $total_seconds | cut -d. -f1 `
        #roudning seconds
        if (( $sim_total_seconds > 60 ));then
            sim_total_minutes=`echo \($sim_total_seconds/60\) +$sim_total_minutes | bc`
            sim_total_seconds=`echo $sim_total_seconds%60 | bc`
        fi
        
#        if (( $sim_total_seconds_sum > 60 ));then
#            sim_total_minutes_sum=`echo \($sim_total_seconds_sum/60\) +$sim_total_minutes_sum | bc`
#            sim_total_seconds_sum=`echo $sim_total_seconds_sum%60 | bc`
#        fi

 #       if (( $total_seconds > 60 ));then
 #           total_minutes=`echo \($total_seconds/60\) +$total_minutes | bc`
 #           total_seconds=`echo $total_seconds%60 | bc`
 #       fi
        
        if (( $time_per_hypo_seconds > 60 ));then
            time_per_hypo_minutes=`echo \($time_per_hypo_seconds/60\) +$time_per_hypo_minutes | bc`
            time_per_hypo_seconds=`echo $time_per_hypo_seconds%60 | bc`
        fi
        #rounding minutes

        if (( $sim_total_minutes > 60 )); then
            sim_total_hours=`echo \($sim_total_minutes/60\) +$sim_total_hours | bc`
            sim_total_minutes=`echo $sim_total_minutes%60 | bc`
        fi
        
#        if (( $sim_total_minutes_sum > 60 )); then
#            sim_total_hours_sum=`echo \($sim_total_minutes_sum/60\) +$sim_total_hours_sum | bc`
#            sim_total_minutes_sum=`echo $sim_total_minutes_sum%60 | bc`
#        fi

#        if (( $total_minutes > 60 ));then
#            total_hours=`echo \($total_minutes/60\) +$total_hours| bc`
#            total_minutes=`echo $total_minutes%60 | bc`
#        fi
        
        if (( $time_per_hypo_minutes > 60 ));then
            time_per_hypo_hours=`echo \($time_per_hypo_minutes/60\) +$time_per_hypo_hours| bc`
            time_per_hypo_minutes=`echo $time_per_hypo_minutes%60 | bc`
        fi
#        total_time=$total_hours:$total_minutes:$total_seconds
        #sim_total_time_sum=$sim_total_hours_sum:$sim_total_minutes_sum:$sim_total_seconds_sum
        sim_total_time=$sim_total_hours:$sim_total_minutes:$sim_total_seconds
        counter=`echo $counter+1 | bc`
#        time_per_hypo=$time_per_hypo_hours:$time_per_hypo_minutes:$time_per_hypo_seconds
        time_per_hypo=`echo $time_per_hypo_hours + \($time_per_hypo_minutes / 60\) + \($time_per_hypo_seconds/3600\) | bc -l `
        time_per_hypo=`printf "%4f\n" $time_per_hypo`

        echo "time_used:  $hours:$minutes:$seconds " >> $logs_path/$sim.log
    done
}

#initializing the total var to store total time used
total_time=`echo 0 |bc `

printf "\r %15s  %15s  %18s  %10s  %6s  %6s  %10s  %10s  %10s  %15s %15s %12s  %12s  %18s\n" 'Rup_mod' 'realizations' 'vm_size' 'duration' 'dt'  'nt' 'fd_count' 'nseg_srf' 'nsub_stoch' 'EMOD3D' 'winbin_aio' 'HF' 'BB' 'total'

for sim in $list_sim;
do
    sim_total_time_sum=`echo 0 | bc`
    #echo running for $sim
    NA=''
    sim_total_time='00:00:00'
    realization_count=0
    #get vm_size
    cd $runs_path/$sim
    vm_size=`python -c "import params_base as vm; print vm.nx+'*'+vm.ny+'*'+vm.nz"`
    sim_duration=`python -c "import params_base as vm; print vm.sim_duration" `
    dt=`echo 0.005`
    nt=`echo $sim_duration / $dt | bc`
    fd_name=`python -c "import params_base as pb; print pb.FD_STATLIST" `
    fd_count=`cat $fd_name| wc -l`
    
    nseg_srf=`python -c "import params_base as params; from qcore.srf import get_nseg; sub_fault_count=get_nseg(params.srf_files[0]);print sub_fault_count"`
#    echo $nseg_srf
    nsub_stoch=`python -c "import params_base as params; from qcore.srf import get_nsub_stoch; sub_fault_count,sub_fault_area=get_nsub_stoch(params.hf_slips[0],get_area=True);print sub_fault_count"`
#    echo $nsub_stoch   

    #get ll related to emod3d
    sequence=`echo 0| bc`
    echo "************EMOD3D************* " >> $logs_path/$sim.log
    for o_file in `ls $runs_path/$sim/run_emod3d.$sim*.out`;
    do
        #save the o_file to the log
        echo "scanning $o_file " >> $logs_path/$sim.log
        #get the line that contains wall_clock_limit
        #get_corehours_used $ll_file
        hypo_name_pre=` echo $o_file | cut -d_ -f3`
        hypo_name_suf=` echo $o_file | cut -d_ -f4 | cut -d. -f1`
        hypo_name=$hypo_name_pre\_$hypo_name_suf
#        echo $hypo_name
        scan_o_file_emod3d $o_file $sequence $hypo_name
        realization_count=`echo $realization_count+1 | bc`
        sequence=`echo sequence + 1| bc`
#        echo $total_time
#        echo $sim_total_time_sum
#        echo $sim_total_time
        #multiply the numbers by 2 for virtual core
        time_per_hypo=`echo $time_per_hypo*2| bc`
        printf "\r %15s  %15s  %18s  %10s  %6s  %6s  %10s  %10s  %10s  %15s  %15s  %12s  %12s  %18s\n" $sim $hypo_name $vm_size $sim_duration $dt $nt $fd_count $nseg_srf $nsub_stoch $time_per_hypo '' '' '' ''
    done
    LF_time=$sim_total_time
    #echo $LF_time
    LF_time_display_m=`echo $LF_time | cut -d: -f2 ` 
    LF_time_display_s=`echo $LF_time | cut -d: -f3 `
    LF_time_display_h=`echo $LF_time | cut -d: -f1 `
    LF_time_display=`echo $LF_time_display_h + \($LF_time_display_m / 60\) + \($LF_time_display_s/3600\) | bc -l`
    LF_time_display=`echo $LF_time_display*2 | bc`
    total_time=`echo $LF_time_display + $total_time | bc`
    sim_total_time_sum=`echo $LF_time_display + $sim_total_time_sum | bc`
    LF_time_display=`printf "%4f\n" $LF_time_display`
    #get ll related to post-emod3d
    sim_total_time='00:00:00'
    echo "*********post_emod3d************ " >> $logs_path/$sim.log
    sequence=`echo 0| bc`
    for o_file in `ls $runs_path/$sim/post_emod3d.winbin_aio.$sim*.out`;
    do
        #log
        echo "scanning $o_file " >> $logs_path/$sim.log
        #get the line that contains wall_clock_limit
        #hypo_name=$hypo_name_pre\_$hypo_name_suf
        #
        #hypo_name=`echo $o_file | cut -d. -f3`
        rup_name_base=`echo $o_file | cut -d_ -f3 | cut -d. -f2`
        hypo_name_pre=`echo $o_file | cut -d_ -f4`
        hypo_name_suf=`echo $o_file | cut -d_ -f5 | cut -d. -f1`
        hypo_name=$rup_name_base\_$hypo_name_pre\_$hypo_name_suf
        srf_name=$hypo_name_pre\_$hypo_name_suf 
#        echo $o_file
#        echo $hypo_name
#        echo "scan_o_file_post_emod3d $o_file $sequence $hypo_name"
#        exit        
        scan_o_file_post_emod3d $o_file $sequence $hypo_name
        sequence=`echo sequence + 1| bc`
        printf "\r %15s  %15s  %18s  %10s  %6s  %6s  %10s  %10s  %10s  %15s  %15s  %12s  %12s  %18s\n" $sim $srf_name $vm_size $sim_duration $dt $nt $fd_count $nseg_srf $nsub_stoch '' $time_per_hypo '' '' ''
    done
    Post_emod_time=$sim_total_time
    #echo $LF_time
    Post_emod_time_display_m=`echo $Post_emod_time | cut -d: -f2 ` 
    Post_emod_time_display_s=`echo $Post_emod_time | cut -d: -f3 `
    Post_emod_time_display_h=`echo $Post_emod_time | cut -d: -f1 `
    Post_emod_time_display=`echo $Post_emod_time_display_h + \($Post_emod_time_display_m / 60\) + \($Post_emod_time_display_s/3600\) | bc -l`
    total_time=`echo $Post_emod_time_display + $total_time | bc`
    sim_total_time_sum=`echo $Post_emod_time_display + $sim_total_time_sum | bc`
    Post_emod_time_display=`printf "%4f\n" $Post_emod_time_display`
#    printf "\r %15s | %15s | %18s | %15s | %12s | %12s | %18s\n" $sim $realization_count $vm_size $LF_time $HF_time $BB_time $sim_total_time_sum
#    exit 1
    #re-init sim_total_tim
    sim_total_time='00:00:00'
    echo "***************HF**************** " >> $logs_path/$sim.log
    #get HF
    for o_file in `ls $runs_path/$sim/sim_hf.*Cant1D_v2-midQ_leer_hfnp2mm+_rvf0p8_sd50_k0p045__$sim_*.out`;
    do
        #log
        echo "scanning $o_file " >> $logs_path/$sim.log 
        #
        hypo_name=` echo $o_file | cut -d. -f2 | cut -d_ -f '9 10 11'`
        srf_name=`echo $hypo_name | cut -d_ -f'2 3'`
        scan_o_file_hf_bb $o_file $hypo_name 
        printf "\r %15s  %15s  %18s  %10s  %6s  %6s  %10s  %10s  %10s  %15s  %15s  %12s  %12s  %18s\n" $sim $srf_name $vm_size $sim_duration $dt $nt $fd_count $nseg_srf $nsub_stoch '' '' $time_per_hypo '' ''
    done
    HF_time=$sim_total_time
    HF_time_display_m=`echo $HF_time | cut -d: -f2 `
    HF_time_display_s=`echo $HF_time | cut -d: -f3 `
    HF_time_display_h=`echo $HF_time | cut -d: -f1 `
    HF_time_display=`echo $HF_time_display_h + \($HF_time_display_m / 60\) + \($HF_time_display_s/3600\) | bc -l`
    total_time=`echo $HF_time_display + $total_time | bc`
    sim_total_time_sum=`echo $HF_time_display + $sim_total_time_sum | bc`
    HF_time_display=`printf "%4f\n" $HF_time_display`
    sim_total_time='00:00:00'
    echo "***************BB**************** " >> $logs_path/$sim.log
    for o_file in `ls $runs_path/$sim/sim_bb_*Cant1D_v2-midQ_leer_hfnp2mm+_rvf0p8_sd50_k0p045__$sim_*.out`;
    do
        #log
        echo "scanning $o_file " >> $logs_path/$sim.log
        #
        hypo_name=` echo $o_file | cut -d. -f2 | cut -d_ -f '9 10 11'`
        scan_o_file_hf_bb $o_file $hypo_name 
        printf "\r %15s  %15s  %18s  %10s  %6s  %6s  %10s  %10s  %10s  %15s  %15s  %12s  %12s  %18s\n" $sim $srf_name $vm_size $sim_duration $dt $nt $fd_count $nseg_srf $nsub_stoch '' '' '' $time_per_hypo ''
#        echo $o_file
    done

    BB_time=$sim_total_time
    BB_time_display_m=`echo $BB_time | cut -d: -f2 ` 
    BB_time_display_s=`echo $BB_time | cut -d: -f3 `
    BB_time_display_h=`echo $BB_time | cut -d: -f1 `
    BB_time_display=`echo $BB_time_display_h + \($BB_time_display_m / 60\) + \($BB_time_display_s/3600\) | bc -l`
    total_time=`echo $BB_time_display + $total_time | bc`
    sim_total_time_sum=`echo $BB_time_display + $sim_total_time_sum | bc`
    sim_total_time_sum=`printf "%4f\n" $sim_total_time_sum`
    BB_time_display=`printf "%4f\n" $BB_time_display`

    #sim_total_time_sum_display_m=`echo $sim_total_time_sum| cut -d: -f2 ` 
    #sim_total_time_sum_display_s=`echo $sim_total_time_sum | cut -d: -f3 `
    #sim_total_time_sum_display_h=`echo $sim_total_time_sum | cut -d: -f1 `
    #sim_total_time_sum_display=`echo $sim_total_time_sum_display_h + \($sim_total_time_sum_display_m / 60\) + \($sim_total_time_sum_display_s/3600\) | bc -l`
    #sim_total_time_sum_display=`printf "%4f\n" $sim_total_time_sum_display`
    printf "\r %15s  %15s  %18s  %10s  %6s  %6s  %10s  %10s  %10s  %15s  %15s  %12s  %12s  %18s\n" $sim $realization_count $vm_size $sim_duration $dt $nt $fd_count $nseg_srf $nsub_stoch $LF_time_display $Post_emod_time_display $HF_time_display $BB_time_display $sim_total_time_sum

done
#total_time=`echo $total_hours + \($total_minutes / 60\) + \($total_seconds/3600\) | bc -l`
total_time=`printf "%4f\n" $total_time`
printf "\r %15s  %15s  %18s  %10s  %6s  %6s  %10s  %10s  %10s  %15s  %15s  %12s  %12s  %18s\n" 'total used' '' '' '' '' '' '' '' '' '' '' '' '' $total_time 
