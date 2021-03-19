#!/bin/bash
# script version: slurm
#
# must be run with sbatch obs_im_calc [observedGroundMotionsDirectory]

#SBATCH --job-name=obs_im_calc
#SBATCH --time=01:00:00
#SBATCH --cpus-per-task=40

export IMPATH=${gmsim}/IM_calculation
export PYTHONPATH=${gmsim}/qcore:/${PYTHONPATH}:${IMPATH}

script_start=`date`
echo "script started running at: $script_start"

obs_dirs=$1
comp="000 090 geom"
comp_count=`echo $comp | awk '{print NF}' `

echo ___calculating observed____

for D in $(echo ${obs_dirs}/*/)
do
    if [[  `find ${D} -name accBB | wc -l` -ge 1 ]]
    then
        fault_name=`basename $D`
        python $gmsim/workflow/scripts/im_calc_checkpoint.py ${obs_dirs}/IM_calc/ $((`ls $D/*/*/accBB | wc -l` / 3)) $comp_count --event_name ${fault_name} --observed
        if [[ $? == 1 ]]; then
            time python ${IMPATH}/calculate_ims.py $D/*/*/accBB a -o ${obs_dirs}/IM_calc/ -np ${SLURM_CPUS_PER_TASK} -i ${fault_name} -r ${fault_name} -c $comp -t o -e -s
        fi
    fi
done

date

