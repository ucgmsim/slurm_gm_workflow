#!/bin/bash
# script version: slurm
#
# must be run with sbatch vm_gen.sl [realisationCSV] [OutputDirectory] [VM_VERSION] [VM_TOPO] [HH] [PGV_THRESHOLD] [DS_MULTIPLIER] [MGMT_DB_LOC] [REL_NAME] [managementDBLocation]

#SBATCH --job-name=VM_PARAMS
#SBATCH --time=00:15:00
#SBATCH --cpus-per-task=1

if [[ -n ${CUR_ENV} && ${CUR_HPC} != "mahuika" ]]; then
    source $CUR_ENV/workflow/install_workflow/helper_functions/activate_env.sh $CUR_ENV "mahuika"
fi

REL_CSV=$1
OUT_DIR=$2
VM_VERSION=$3
VM_TOPO=$4
HH=$5
PGV_THRESHOLD=$6
DS_MULTIPLIER=$7
MGMT_DB_LOC=$8
REL_NAME=$9



if [[ ! -d $OUT_DIR ]]; then
    mkdir -p $OUT_DIR
fi

#updating the stats in managementDB
if [[ ! -d $MGMT_DB_LOC/mgmt_db_queue ]]; then
    #create the queue folder if not exist
    mkdir $MGMT_DB_LOC/mgmt_db_queue
fi
timestamp=`date +%Y%m%d_%H%M%S`
python $gmsim/workflow/scripts/cybershake/add_to_mgmt_queue.py $MGMT_DB_LOC/mgmt_db_queue $REL_NAME VM_PARAMS running $SLURM_JOB_ID

runtime_fmt="%Y-%m-%d_%H:%M:%S"
start_time=`date +$runtime_fmt`
echo $start_time

echo "python $gmsim/Pre-processing/VM/rel2vm_params.py -o $OUT_DIR --hh $HH --vm-version $VM_VERSION --vm-topo  $VM_TOPO --pgv $PGV_THRESHOLD --ds-multiplier $DS_MULTIPLIER $REL_CSV"
python $gmsim/Pre-processing/VM/rel2vm_params.py -o $OUT_DIR --hh $HH --vm-version $VM_VERSION --vm-topo  $VM_TOPO --pgv $PGV_THRESHOLD --ds-multiplier $DS_MULTIPLIER $REL_CSV

end_time=`date +$runtime_fmt`
echo $end_time

timestamp=`date +%Y%m%d_%H%M%S`
if [[ -f $OUT_DIR/vm_params.yaml ]]; then
    #passed

    python $gmsim/workflow/scripts/cybershake/add_to_mgmt_queue.py $MGMT_DB_LOC/mgmt_db_queue $REL_NAME VM_PARAMS completed $SLURM_JOB_ID

    if [[ ! -d $REL_LOC/ch_log ]]; then
        mkdir $REL_LOC/ch_log
    fi

    # save meta data
    python $gmsim/workflow/metadata/log_metadata.py $REL_LOC VM_PARAMS cores=$SLURM_NTASKS start_time=$start_time end_time=$end_time
else
    #reformat $res to remove '\n'
    res=`echo $res | tr -d '\n'`
    python $gmsim/workflow/scripts/cybershake/add_to_mgmt_queue.py $MGMT_DB_LOC/mgmt_db_queue $REL_NAME VM_PARAMS failed $SLURM_JOB_ID --error "$res"
fi
