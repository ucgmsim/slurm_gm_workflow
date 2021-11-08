#!/bin/bash
# script version: slurm
#
# must be run with sbatch install_fault.sl [VM_PARAMS] [STAT_FILE] [FAULT_DIR] [FDSTATLIST] [MGMT_DB_LOC] [REL_NAME]

#SBATCH --job-name=install_fault
#SBATCH --time=00:15:00
#SBATCH --cpus-per-task=1

if [[ -n ${CUR_ENV} && ${CUR_HPC} != "mahuika" ]]; then
    source $CUR_ENV/workflow/install_workflow/helper_functions/activate_env.sh $CUR_ENV "mahuika"
fi

VM_PARAMS=${1:?VM_PARAMS argument missing}
STAT_FILE=${2:?STAT_FILE argument missing}
FAULT_DIR=${3:?FAULT_DIR argument missing}
FDSTATLIST=${4:?FDSTATLIST argument missing}
MGMT_DB_LOC=${5:?MGMT_DB_LOC argument missing}
REL_NAME=${6:?REL_NAME argument missing}



FAULT=$(echo $REL_NAME | cut -d"_" -f1)
SIM_DIR=$MGMT_DB_LOC/Runs/$FAULT/$REL_NAME
CH_LOG_FFP=$SIM_DIR/ch_log


if [[ ! -d $FAULT_DIR ]]; then
    mkdir -p $FAULT_DIR
fi

#updating the stats in managementDB
if [[ ! -d $MGMT_DB_LOC/mgmt_db_queue ]]; then
    #create the queue folder if not exist
    mkdir $MGMT_DB_LOC/mgmt_db_queue
fi
timestamp=`date +%Y%m%d_%H%M%S`
python $gmsim/workflow/scripts/cybershake/add_to_mgmt_queue.py $MGMT_DB_LOC/mgmt_db_queue $REL_NAME INSTALL_FAULT running $SLURM_JOB_ID

runtime_fmt="%Y-%m-%d_%H:%M:%S"
start_time=`date +$runtime_fmt`
echo $start_time

echo python $gmsim/workflow/shared_workflow/gen_fd.py $VM_PARAMS $STAT_FILE $FAULT_DIR
python $gmsim/workflow/shared_workflow/gen_fd.py $VM_PARAMS $STAT_FILE $FAULT_DIR

end_time=`date +$runtime_fmt`
echo $end_time

#TODO do more unit, such as sim_duration

timestamp=`date +%Y%m%d_%H%M%S`
if [[ -f $FDSTATLIST ]]; then
    #passed

    python $gmsim/workflow/scripts/cybershake/add_to_mgmt_queue.py $MGMT_DB_LOC/mgmt_db_queue $REL_NAME INSTALL_FAULT completed $SLURM_JOB_ID

    if [[ ! -d $CH_LOG_FFP ]]; then
        mkdir $CH_LOG_FFP
    fi

    # save meta data
    python $gmsim/workflow/metadata/log_metadata.py $SIM_DIR INSTALL_FAULT cores=$SLURM_CPUS_PER_TASK start_time=$start_time end_time=$end_time
else
    #reformat $res to remove '\n'
    res=`echo $res | tr -d '\n'`
    python $gmsim/workflow/scripts/cybershake/add_to_mgmt_queue.py $MGMT_DB_LOC/mgmt_db_queue $REL_NAME INSTALL_FAULT failed $SLURM_JOB_ID --error "$res"
fi
