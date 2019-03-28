#!/usr/bin/env bash
# script version: slurm
#
# must be run with sbatch clean_up.sl [realisation directory] [realisation name] [management database location]

#SBATCH --job-name=clean_up
#SBATCH --account=nesi00213
#SBATCH --partition=prepost
#SBATCH --time=00:30:00
#SBATCH --cpus-per-task=1

SIM_DIR=$1
SRF_NAME=$2
MGMT_DB_LOC=$3

script_start=`date`
echo "script started running at: $script_start"
runtime_fmt="%Y-%m-%d_%H:%M:%S"
timestamp=`date +%Y%m%d_%H%M%S`

start_time=`date +${runtime_fmt}`
echo ___cleaning up___
echo "python $gmsim/workflow/scripts/management/update_mgmt_db.py $MGMT_DB_LOC clean_up running --run_name $SRF_NAME --job $SLURM_JOBID" >> $MGMT_DB_LOC/mgmt_db_queue/$timestamp\_$SLURM_JOBID

res=`time python $gmsim/workflow/scripts/clean_up.py $SIM_DIR`

exit_val=$?

end_time=`date +$runtime_fmt`
echo $end_time

if [[ $exit_val == 0 ]]; then
    #passed
    echo "python $gmsim/workflow/scripts/management/update_mgmt_db.py $MGMT_DB_LOC clean_up completed --run_name $SRF_NAME --force " >> $MGMT_DB_LOC/mgmt_db_queue/$timestamp\_$SLURM_JOBID

    #save meta data
    python $gmsim/workflow/metadata/log_metadata.py $SIM_DIR clean_up start_time=$start_time end_time=$end_time

else
    echo "python $gmsim/workflow/scripts/management/update_mgmt_db.py $MGMT_DB_LOC clean_up failed --run_name $SRF_NAME --error '$res' --force" >> $MGMT_DB_LOC/mgmt_db_queue/$timestamp\_$SLURM_JOBID
fi