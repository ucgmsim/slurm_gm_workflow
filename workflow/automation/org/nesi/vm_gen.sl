#!/bin/bash
# script version: slurm
#
# must be run with sbatch vm_gen.sl [VM_PARAMS_YAML] [OUT_DIR] [SRF]  [MGMT_DB_LOC] [REL_NAME]

##SBATCH --partition=milan
#SBATCH --job-name=VM_GEN
#SBATCH --time=01:00:00
#SBATCH --cpus-per-task=32

if [[ -n ${CUR_ENV} && ${CUR_HPC} != "mahuika" ]]; then
    source $CUR_ENV/workflow/workflow/environments/helper_functions/activate_env.sh $CUR_ENV "mahuika"
fi

VM_PARAMS_YAML=${1:?VM_PARAMS_YAML argument missing}
OUT_DIR=${2:?OUT_DIR argument missing}
SRF=${3:?SRF argument missing}
MGMT_DB_LOC=${4:?MGMT_DB_LOC argument missing}
REL_NAME=${5:?REL_NAME argument missing}

FAULT=$(echo $REL_NAME | cut -d"_" -f1)
SIM_DIR=$MGMT_DB_LOC/Runs/$FAULT/$REL_NAME
CH_LOG_FFP=$SIM_DIR/ch_log

if [[ ! -d $OUT_DIR ]]; then
    mkdir -p $OUT_DIR
fi

#updating the stats in managementDB
if [[ ! -d $MGMT_DB_LOC/mgmt_db_queue ]]; then
    #create the queue folder if not exist
    mkdir $MGMT_DB_LOC/mgmt_db_queue
fi
timestamp=`date +%Y%m%d_%H%M%S`
runtime_fmt="%Y-%m-%d_%H:%M:%S"
start_time=`date +$runtime_fmt`
echo $start_time

python $gmsim/workflow/workflow/automation/execution_scripts/add_to_mgmt_queue.py $MGMT_DB_LOC/mgmt_db_queue $REL_NAME VM_GEN running $SLURM_JOB_ID --start_time "$start_time" --nodes $SLURM_NNODES --cores $SLURM_CPUS_PER_TASK --wct 01:00:00

echo python $gmsim/Pre-processing/VM/vm_params2vm.py -t 18 $REL_NAME $VM_PARAMS_YAML -o $OUT_DIR
python $gmsim/Pre-processing/VM/vm_params2vm.py -t 18 $REL_NAME $VM_PARAMS_YAML -o $OUT_DIR

chmod g+rwXs -R $OUT_DIR
chgrp nesi00213 -R $OUT_DIR

end_time=`date +$runtime_fmt`
echo $end_time

timestamp=`date +%Y%m%d_%H%M%S`
#test before update
res=`python $gmsim/qcore/qcore/validate_vm.py NZVM $OUT_DIR`
pass=$?

if [[ $pass == 0 ]]; then
    #passed

    python $gmsim/workflow/workflow/automation/execution_scripts/add_to_mgmt_queue.py $MGMT_DB_LOC/mgmt_db_queue $REL_NAME VM_GEN completed $SLURM_JOB_ID --end_time "$end_time"

    if [[ ! -d $CH_LOG_FFP ]]; then
        mkdir $CH_LOG_FFP
    fi

    # save meta data
    python $gmsim/workflow/workflow/automation/metadata/log_metadata.py $SIM_DIR VM_GEN cores=$SLURM_CPUS_PER_TASK start_time=$start_time end_time=$end_time
else
    #reformat $res to remove '\n'
    res=`echo $res | tr -d '\n'`
    python $gmsim/workflow/workflow/automation/execution_scripts/add_to_mgmt_queue.py $MGMT_DB_LOC/mgmt_db_queue $REL_NAME VM_GEN failed $SLURM_JOB_ID --error "$res" --end_time "$end_time"
fi
