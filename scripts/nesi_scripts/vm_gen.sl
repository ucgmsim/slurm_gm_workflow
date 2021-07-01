#!/bin/bash
# script version: slurm
#
# must be run with sbatch vm_gen.sl [realisationDirectory] [OutputDirectory] [managementDBLocation]

#SBATCH --job-name=VM_GEN
#SBATCH --time=01:00:00
#SBATCH --cpus-per-task=18

if [[ -n ${CUR_ENV} && ${CUR_HPC} != "mahuika" ]]; then
    source $CUR_ENV/workflow/install_workflow/helper_functions/activate_env.sh $CUR_ENV "mahuika"
fi

VM_PARAMS_YAML=$1
OUT_DIR=$2
SRF=$3
MGMT_DB_LOC=$4
REL_NAME=$5

FAULT=$(echo $REL_NAME | cut -d"_" -f1)
SIM_DIR=$MGMT_DB_LOC/Runs/$FAULT/$REL_NAME/
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
python $gmsim/workflow/scripts/cybershake/add_to_mgmt_queue.py $MGMT_DB_LOC/mgmt_db_queue $REL_NAME VM_GEN running $SLURM_JOB_ID

runtime_fmt="%Y-%m-%d_%H:%M:%S"
start_time=`date +$runtime_fmt`
echo $start_time

echo "python $gmsim/Pre-processing/VM/vm_params2vm.py -t 18 $REL_NAME $VM_PARAMS_YAML -o $OUT_DIR"
python $gmsim/Pre-processing/VM/vm_params2vm.py -t 18 $REL_NAME $VM_PARAMS_YAML -o $OUT_DIR

chmod g+rwXs -R $OUT_DIR
chgrp nesi00213 -R $OUT_DIR

end_time=`date +$runtime_fmt`
echo $end_time

timestamp=`date +%Y%m%d_%H%M%S`
#test before update
res=`python $gmsim/qcore/qcore/validate_vm.py $OUT_DIR --srf $SRF`
if [[ $? == 0 ]]; then
    #passed

    python $gmsim/workflow/scripts/cybershake/add_to_mgmt_queue.py $MGMT_DB_LOC/mgmt_db_queue $REL_NAME VM_GEN completed $SLURM_JOB_ID

    if [[ ! -d $CH_LOG_FFP ]]; then
        mkdir $CH_LOG_FFP
    fi

    # save meta data
    python $gmsim/workflow/metadata/log_metadata.py $SIM_DIR VM_GEN cores=$SLURM_CPUS_PER_TASK start_time=$start_time end_time=$end_time
else
    #reformat $res to remove '\n'
    res=`echo $res | tr -d '\n'`
    python $gmsim/workflow/scripts/cybershake/add_to_mgmt_queue.py $MGMT_DB_LOC/mgmt_db_queue $REL_NAME VM_GEN failed $SLURM_JOB_ID --error "$res"
fi
