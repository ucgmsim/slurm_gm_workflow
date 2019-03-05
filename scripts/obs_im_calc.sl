#!/bin/bash
# script version: slurm
#

#SBATCH --job-name=obs_im_calc
#SBATCH --account=nesi00213
#SBATCH --partition=nesi_research
#SBATCH --time=01:00:00
#SBATCH --cpus-per-task=40

export IMPATH=$gmsim/IM_calculation
export PYTHONPATH=$gmsim/qcore:/$PYTHONPATH:$IMPATH

script_start=`date`
echo "script started running at: $script_start"

obs_dirs=$1

echo ___calculating observed____

for D in `find . -type d`
do
    if [[  `find $D -maxdepth 3 -mindepth 3 -name "accBB" | wc -l` -ge 1 ]]
    then
        fault_name=`basename $D`
        time python $IMPATH/calculate_ims.py $D/*/*/accBB a -o $obs_dirs/IM_calc/ -np $SLURM_NTASKS -i $fault_name -r $fault_name -c geom -t o -e -s
    fi
done

date

