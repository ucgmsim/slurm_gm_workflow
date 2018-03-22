#!/bin/bash

#script needs one arg(emod3d,post-emod, hf, or bb)
if [[ $# -lt 1 ]];then
    echo "please provide the jobs you want to submit (emod3d,post_emod3d, hf, or bb)."
    exit
fi
job=$1
#use ls to test if the desired submit script exsist
ls $gmsim/workflow/scripts/submit_$job.py 2>1 1>/dev/null
#
if [[ $? -ne 0 ]];then
    echo "Error: cannot find script for $job"
    exit 1
fi

#get $BINPROCESS from machine_env.sh
source machine_env.sh


#submit emod3d
if [[ $job = emod3d ]]; then
    echo submit_emod3d
    python $BINPROCESS/submit_emod3d.py
elif [[ $job = post_emod3d ]]; then
    echo submit_post_emod3d
    python $BINPROCESS/submit_post_emod3d.py
elif [[ $job = hf ]]; then
    echo submit_hf
    python $BINPROCESS/submit_hf.py
elif [[ $jon = bb ]]; then
    python $BINPROCESS/submit_bb.py
fi
