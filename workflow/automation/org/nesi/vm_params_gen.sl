#!/bin/bash
# script version: slurm
#
# must be run with sbatch vm_gen.sl [realisationCSV] [OUTPUT_DIR] [VM_VERSION] [VM_TOPO] [HH] [PGV_THRESHOLD] [DS_MULTIPLIER] [MGMT_DB_LOC] [REL_NAME] [MGMT_DB_LOC]

#SBATCH --job-name=VM_PARAMS
#SBATCH --time=00:15:00
#SBATCH --cpus-per-task=1

if [[ -n ${CUR_ENV} && ${CUR_HPC} != "mahuika" ]]; then
    source $CUR_ENV/workflow/workflow/environments/helper_functions/activate_env.sh $CUR_ENV "mahuika"
fi

REL_CSV=${1:?realisationCSV argument missing}
OUT_DIR=${2:?OUTPUT_DIR argument missing}
VM_VERSION=${3:?VM_VERSION argument missing}
VM_TOPO=${4:?VM_TOPO argument missing}
HH=${5:?HH argument missing}
PGV_THRESHOLD=${6:?PGV_THRESHOLD argument missing}
DS_MULTIPLIER=${7:?DS_MULTIPLIER argument missing}
MGMT_DB_LOC=${8:?MGMT_DB_LOC argument missing}
REL_NAME=${9:?REL_NAME argument missing}

if [[ $# != 9 ]]; then
    echo "9 arguments must be provided, please adjust and re-run"
    exit
fi

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
python $gmsim/workflow/workflow/automation/execution_scripts/add_to_mgmt_queue.py $MGMT_DB_LOC/mgmt_db_queue $REL_NAME VM_PARAMS running $SLURM_JOB_ID

runtime_fmt="%Y-%m-%d_%H:%M:%S"
start_time=`date +$runtime_fmt`
echo $start_time

echo "python $gmsim/Pre-processing/VM/rel2vm_params.py -o $OUT_DIR --hh $HH --vm-version $VM_VERSION --vm-topo  $VM_TOPO --pgv $PGV_THRESHOLD --ds-multiplier $DS_MULTIPLIER $REL_CSV"
python $gmsim/Pre-processing/VM/rel2vm_params.py -o $OUT_DIR --hh $HH --vm-version $VM_VERSION --vm-topo  $VM_TOPO --pgv $PGV_THRESHOLD --ds-multiplier $DS_MULTIPLIER $REL_CSV

end_time=`date +$runtime_fmt`
echo $end_time

timestamp=`date +%Y%m%d_%H%M%S`
res=`python $gmsim/qcore/qcore/validate_vm.py params $OUT_DIR/vm_params.yaml`
pass=$?

if [[ $pass == 0 ]]; then
    #passed

    python $gmsim/workflow/workflow/automation/execution_scripts/add_to_mgmt_queue.py $MGMT_DB_LOC/mgmt_db_queue $REL_NAME VM_PARAMS completed $SLURM_JOB_ID --start_time "$start_time" --end_time "$end_time" --nodes $SLURM_NNODES --cores $SLURM_CPUS_PER_TASK --wct 00:15:00

    if [[ ! -d $CH_LOG_FFP ]]; then
        mkdir $CH_LOG_FFP
    fi

    # save meta data
    python $gmsim/workflow/workflow/automation/metadata/log_metadata.py $SIM_DIR VM_PARAMS cores=$SLURM_CPUS_PER_TASK start_time=$start_time end_time=$end_time
else
    #reformat $res to remove '\n'
    res=`echo $res | tr -d '\n'`
    python $gmsim/workflow/workflow/automation/execution_scripts/add_to_mgmt_queue.py $MGMT_DB_LOC/mgmt_db_queue $REL_NAME VM_PARAMS failed $SLURM_JOB_ID --error "$res" --start_time "$start_time" --end_time "$end_time" --nodes $SLURM_NNODES --cores $SLURM_CPUS_PER_TASK --wct 00:15:00
fi
