#!/bin/bash
# script version: slurm
#
# must be run with sbatch vm_gen.sl [realisationDirectory] [OutputDirectory] [managementDBLocation]

#SBATCH --job-name=VM_GEN
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
REL_NAME=$6
MGMT_DB_LOC=$7



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

echo "python $gmsim/Pre-processing/VM/rel2vm_params.py --hh $HH --vm-version $VM_VERSION --vm-topo  $VM_TOPO --pgv 2.0 --ds-multiplier 0.75 $REL_CSV"
python $gmsim/Pre-processing/VM/rel2vm_params.py --hh $HH --vm-version $VM_VERSION --vm-topo  $VM_TOPO --pgv 2.0 --ds-multiplier 0.75 $REL_CSV

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
