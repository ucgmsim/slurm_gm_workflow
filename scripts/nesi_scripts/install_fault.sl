#!/bin/bash
# script version: slurm
#
# must be run with sbatch install_fault.sl [realisationDirectory] [OutputDirectory] [managementDBLocation]

#SBATCH --job-name=fault_install
#SBATCH --time=00:15:00
#SBATCH --cpus-per-task=1

if [[ -n ${CUR_ENV} && ${CUR_HPC} != "mahuika" ]]; then
    source $CUR_ENV/workflow/install_workflow/helper_functions/activate_env.sh $CUR_ENV "mahuika"
fi
VM_PARAMS=$1
STAT_FILE=$2
FAULT_DIR=$3
REL_NAME=$6
MGMT_DB_LOC=$7



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

python $gmsim/workflow/shared_workflow/gen_fd.py $VM_PARAMS $STAT_FILE $FAULT_DIR

end_time=`date +$runtime_fmt`
echo $end_time

timestamp=`date +%Y%m%d_%H%M%S`
if [[ -f $OUT_DIR/vm_params.yaml ]]; then
    #passed

    python $gmsim/workflow/scripts/cybershake/add_to_mgmt_queue.py $MGMT_DB_LOC/mgmt_db_queue $REL_NAME INSTALL_FAULT completed $SLURM_JOB_ID

    if [[ ! -d $REL_LOC/ch_log ]]; then
        mkdir $REL_LOC/ch_log
    fi

    # save meta data
    python $gmsim/workflow/metadata/log_metadata.py $REL_LOC VM_PARAMS cores=$SLURM_NTASKS start_time=$start_time end_time=$end_time
else
    #reformat $res to remove '\n'
    res=`echo $res | tr -d '\n'`
    python $gmsim/workflow/scripts/cybershake/add_to_mgmt_queue.py $MGMT_DB_LOC/mgmt_db_queue $REL_NAME INSTALL_FAULT failed $SLURM_JOB_ID --error "$res"
fi
