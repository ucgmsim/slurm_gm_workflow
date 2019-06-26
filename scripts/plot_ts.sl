#!/usr/bin/env bash
# script version: slurm
#
# must be run with sbatch plot_ts.sl [xyts file path] [srf file path] [output ts file path] [management database location] [realization name]

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
OUTPUT_TS_PATH=$3
MGMT_DB_LOC=$4
SRF_NAME=$5

script_start=`date`
echo "script started running at: $script_start"
runtime_fmt="%Y-%m-%d_%H:%M:%S"
timestamp=`date +%Y%m%d_%H%M%S`

start_time=`date +${runtime_fmt}`
echo ___plotting ts___

python $gmsim/workflow/scripts/cybershake/add_to_mgmt_queue.py $MGMT_DB_LOC/mgmt_db_queue $SRF_NAME plot_ts running
res=`python $gmsim/visualization/gmt/plot_ts.py $XYTS_PATH --srf $SRF_PATH --output $OUTPUT_TS_PATH -n 36`
exit_val=$?

end_time=`date +$runtime_fmt`
echo $end_time

if [[ $exit_val == 0 ]]; then
    #passed

    python $gmsim/workflow/scripts/cybershake/add_to_mgmt_queue.py $MGMT_DB_LOC/mgmt_db_queue $SRF_NAME plot_ts completed

else
    python $gmsim/workflow/scripts/cybershake/add_to_mgmt_queue.py $MGMT_DB_LOC/mgmt_db_queue $SRF_NAME plot_ts failed --error "$res"
fi
