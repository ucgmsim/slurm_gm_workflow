#!/bin/bash
# script version: slurm
#
# must be run with sbatch install_fault.sl [VM_PARAMS] [STAT_FILE] [FAULT_DIR] [FDSTATLIST] [MGMT_DB_LOC] [REL_NAME]

#SBATCH --job-name=install_fault
#SBATCH --time=00:15:00
#SBATCH --cpus-per-task=1

if [[ -n ${CUR_ENV} && ${CUR_HPC} != "mahuika" ]]; then
    source $CUR_ENV/workflow/workflow/environments/helper_functions/activate_env.sh $CUR_ENV "mahuika"
fi


FAULT=${1:?FAULT argument missing}
MGMT_DB_LOC=${2:?MGMT_DB_LOC argument missing}


SIM_DIR=$SIMULATION_ROOT/Runs/$FAULT/$FAULT
CH_LOG_FFP=$SIM_DIR/ch_log


mkdir -p $SIM_DIR
mkdir -p $SIMULATION_ROOT/mgmt_db_queue

timestamp=`date +%Y%m%d_%H%M%S`
runtime_fmt="%Y-%m-%d_%H:%M:%S"
start_time=`date +$runtime_fmt`
echo $start_time

python $gmsim/workflow/workflow/automation/execution_scripts/add_to_mgmt_queue.py SIMULATION_ROOT/mgmt_db_queue $FAULT INSTALL_FAULT running $SLURM_JOB_ID --start_time "$start_time" --nodes $SLURM_NNODES --cores $SLURM_CPUS_PER_TASK --wct 00:15:00

CMD="python $gmsim/workflow/workflow/automation/install_scripts/install_fault.py $SIMULATION_ROOT $FAULT"
echo "${CMD}"
$CMD

end_time=`date +$runtime_fmt`
echo $end_time

#TODO do more testing, such as sim_duration

timestamp=`date +%Y%m%d_%H%M%S`
if [[ -f $FDSTATLIST ]]; then
    #passed

    python $gmsim/workflow/workflow/automation/execution_scripts/add_to_mgmt_queue.py SIMULATION_ROOT/mgmt_db_queue $REL_NAME INSTALL_FAULT completed $SLURM_JOB_ID --end_time "$end_time"

    if [[ ! -d $CH_LOG_FFP ]]; then
        mkdir $CH_LOG_FFP
    fi

    # save meta data
    python $gmsim/workflow/workflow/automation/metadata/log_metadata.py $SIM_DIR INSTALL_FAULT cores=$SLURM_CPUS_PER_TASK start_time=$start_time end_time=$end_time
else
    #reformat $res to remove '\n'
    res=`echo $res | tr -d '\n'`
    python $gmsim/workflow/workflow/automation/execution_scripts/add_to_mgmt_queue.py SIMULATION_ROOT/mgmt_db_queue $REL_NAME INSTALL_FAULT failed $SLURM_JOB_ID --error "$res" --end_time "$end_time"
fi
