#!/usr/bin/env bash
# script version: slurm
#
# must be run with sbatch clean_up.sl [realisation directory] [realisation name] [management database location]

#SBATCH --job-name=plot_ts
#SBATCH --account=nesi00213
#SBATCH --partition=prepost
#SBATCH --time=00:30:00
#SBATCH --cpus-per-task=36

if [[ ! -z ${CUR_ENV} && ${CUR_HPC} != "mahuika" ]]; then
    source $CUR_ENV/workflow/install_workflow/helper_functions/activate_env.sh $CUR_ENV "mahuika"
fi

XYTS_PATH=$1
SRF_PATH=$2
MGMT_DB_LOC=$3

script_start=`date`
echo "script started running at: $script_start"
runtime_fmt="%Y-%m-%d_%H:%M:%S"
timestamp=`date +%Y%m%d_%H%M%S`

start_time=`date +${runtime_fmt}`
echo ___plotting ts___

python $gmsim/workflow/scripts/cybershake/add_to_mgmt_queue.py $MGMT_DB_LOC/mgmt_db_queue $XYTS_PATH $SRF_PATH plot_ts running
res=`python $gmsim/visualization/gmt/plot_ts.py $XYTS_PATH --srf $SRF_PATH`
exit_val=$?

end_time=`date +$runtime_fmt`
echo $end_time

if [[ $exit_val == 0 ]]; then
    #passed

    python $gmsim/workflow/scripts/cybershake/add_to_mgmt_queue.py $MGMT_DB_LOC/mgmt_db_queue $XYTS_PATH $SRF_PATH plot_ts completed

else
    python $gmsim/workflow/scripts/cybershake/add_to_mgmt_queue.py $MGMT_DB_LOC/mgmt_db_queue $XYTS_PATH $SRF_PATH plot_ts --error "$res"
fi