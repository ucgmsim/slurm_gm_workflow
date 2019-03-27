#!/usr/bin/env bash
# script version: slurm
#
# must be run with sbatch clean_up.sl [realisation directory]

#SBATCH --job-name=clean_up
#SBATCH --account=nesi00213
#SBATCH --partition=prepost
#SBATCH --time=01:00:00
#SBATCH --cpus-per-task=1

SIM_DIR=$1
SRF_NAME=$2
MGMT_DB_LOC=$3

echo "python $gmsim/workflow/scripts/management/update_mgmt_db.py $MGMT_DB_LOC clean_up running --run_name $SRF_NAME --job $SLURM_JOBID" >> $MGMT_DB_LOC/mgmt_db_queue/$timestamp\_$SLURM_JOBID

python $gmsim/workflow/scripts/clean_up.py $SIM_DIR

if [[ $? == 0 ]]; then
    #passed
    echo "python $gmsim/workflow/scripts/management/update_mgmt_db.py $MGMT_DB_LOC clean_up completed --run_name $SRF_NAME --force " >> $MGMT_DB_LOC/mgmt_db_queue/$timestamp\_$SLURM_JOBID

    #save meta data
    python $gmsim/workflow/metadata/log_metadata.py $SIM_DIR clean_up start_time=$start_time end_time=$end_time

else
    echo "python $gmsim/workflow/scripts/management/update_mgmt_db.py $MGMT_DB_LOC clean_up failed --run_name $SRF_NAME --error '$res' --force" >> $MGMT_DB_LOC/mgmt_db_queue/$timestamp\_$SLURM_JOBID
fi