#!/usr/bin/env bash
#
# must be run with bash clean_up.sh [realisation directory] [realisation name] [management database location]

SIM_DIR=$1
SRF_NAME=$2
MGMT_DB_LOC=$3

script_start=`date`
echo "script started running at: $script_start"
runtime_fmt="%Y-%m-%d_%H:%M:%S"
timestamp=`date +%Y%m%d_%H%M%S`

start_time=`date +${runtime_fmt}`
echo ___cleaning up___

rm -r $SIM_DIR/LF/Restart
res=`python $gmsim/workflow/workflow/scripts/clean_up.py $SIM_DIR`
exit_val=$?

end_time=`date +$runtime_fmt`
echo $end_time

if [[ $exit_val == 0 ]]; then
    #passed

    python $gmsim/workflow/workflow/automation/execution_scripts/add_to_mgmt_queue.py $MGMT_DB_LOC/mgmt_db_queue $SRF_NAME clean_up completed $SLURM_JOB_ID

    #save meta data
    python $gmsim/workflow/workflow/automation/metadata/log_metadata.py $SIM_DIR clean_up start_time=$start_time end_time=$end_time

else
    python $gmsim/workflow/workflow/automation/execution_scripts/add_to_mgmt_queue.py $MGMT_DB_LOC/mgmt_db_queue $SRF_NAME clean_up failed $SLURM_JOB_ID --error "$res"
fi
