#!/usr/bin/env bash
# script version: slurm
#
# must be run with sbatch clean_up.sl [realisation directory] [realisation name] [management database location]

#SBATCH --job-name=clean_up
#SBATCH --account=nesi00213
#SBATCH --partition=prepost
#SBATCH --time=00:30:00
#SBATCH --cpus-per-task=1

if [[ ! -z ${CUR_ENV} && ${CUR_HPC} != "mahuika" ]]; then
    source $CUR_ENV/workflow/install_workflow/helper_functions/activate_env.sh $CUR_ENV "mahuika"
fi

SIM_DIR=$1
SRF_NAME=$2
MGMT_DB_LOC=$3

script_start=`date`
echo "script started running at: $script_start"
runtime_fmt="%Y-%m-%d_%H:%M:%S"
timestamp=`date +%Y%m%d_%H%M%S`

start_time=`date +${runtime_fmt}`
echo ___cleaning up___

python $gmsim/workflow/scripts/cybershake/add_to_mgmt_queue.py $MGMT_DB_LOC/mgmt_db_queue $SRF_NAME clean_up running
rm -r $SIM_DIR/LF/Restart
res=`python $gmsim/workflow/scripts/clean_up.py $SIM_DIR`
exit_val=$?

end_time=`date +$runtime_fmt`
echo $end_time

if [[ $exit_val == 0 ]]; then
    #passed

    python $gmsim/workflow/scripts/cybershake/add_to_mgmt_queue.py $MGMT_DB_LOC/mgmt_db_queue $SRF_NAME clean_up completed

    #save meta data
    python $gmsim/workflow/metadata/log_metadata.py $SIM_DIR clean_up start_time=$start_time end_time=$end_time

else
    python $gmsim/workflow/scripts/cybershake/add_to_mgmt_queue.py $MGMT_DB_LOC/mgmt_db_queue $SRF_NAME clean_up failed --error "$res"
fi