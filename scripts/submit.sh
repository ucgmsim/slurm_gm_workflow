#!/bin/bash
BINPROCESS=$gmsim/workflow/scripts
BINPROCESS=/home/melody.zhu/slurm_gm_workflow/scripts
#script needs one arg(emod3d,post-emod, hf, or bb)
if [[ $# -lt 1 ]];then
    echo "please provide the jobs you want to submit (emod3d,post_emod3d, hf, or bb)."
    exit
fi
job=$1
#use ls to test if the desired submit script exsist
ls $BINPROCESS/submit_$job.py 2>&1 1>/dev/null
#
if [[ $? -ne 0 ]];then
    echo "Error: cannot find script for $job"
    exit 1
fi

#a quick hack to store all the extra args for the script
count=0
for args in $@
do
    #ignore the first arg
    if [[ $count == 0 ]];then
        count=`echo $count+1 | bc`
        continue
    fi
    additional_args="$additional_args $args"
done

#submit emod3d
if [[ $job = emod3d ]]; then
    echo submit_emod3d
    python $BINPROCESS/submit_emod3d.py $additional_args
elif [[ $job = post_emod3d ]]; then
    echo submit_post_emod3d
    python $BINPROCESS/submit_post_emod3d.py $additional_args
elif [[ $job = hf ]]; then
    echo submit_hf
    python $BINPROCESS/submit_hf.py $additional_args
elif [[ $job = bb ]]; then
    python $BINPROCESS/submit_bb.py $additional_args
fi
