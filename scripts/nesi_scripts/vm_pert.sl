#!/bin/bash
# script version: slurm
#
# must be run with sbatch vm_gen.sl [realisationDirectory] [OutputDirectory] [managementDBLocation]

#SBATCH --job-name=VM_PERT
#SBATCH --time=02:00:00
#SBATCH --cpus-per-task=1

if [[ -n ${CUR_ENV} && ${CUR_HPC} != "mahuika" ]]; then
    source $CUR_ENV/workflow/install_workflow/helper_functions/activate_env.sh $CUR_ENV "mahuika"
fi

VM_PARAMS_YAML=$1
OUT_DIR=$2
SRF=$3
MGMT_DB_LOC=$4
REL_NAME=$5

FAULT=$(echo $REL_NAME | cut -d"_" -f1)
CH_LOG_FFP=$MGMT_DB_LOC/$FAULT/$REL_NAME/ch_log


if [[ ! -d $OUT_DIR ]]; then
    mkdir -p $OUT_DIR
fi

#updating the stats in managementDB
if [[ ! -d $MGMT_DB_LOC/mgmt_db_queue ]]; then
    #create the queue folder if not exist
    mkdir $MGMT_DB_LOC/mgmt_db_queue
fi
timestamp=`date +%Y%m%d_%H%M%S`
python $gmsim/workflow/scripts/cybershake/add_to_mgmt_queue.py $MGMT_DB_LOC/mgmt_db_queue $REL_NAME VM_PERT running $SLURM_JOB_ID

runtime_fmt="%Y-%m-%d_%H:%M:%S"
start_time=`date +$runtime_fmt`
echo $start_time

echo "time python $gmsim/Pre-processing/srf_generation/velocity_model_generation/generate_perturbation_file.py $OUT_DIR/$REL_NAME.pertb $VM_PARAMS_YAML -n 1 --perturbation --model $gmsim/Pre-processing/srf_generation/velocity_model_generation/config_files/graves_pitarka_2016_model_modified.csv"
time python $gmsim/Pre-processing/srf_generation/velocity_model_generation/generate_perturbation_file.py $OUT_DIR/$REL_NAME.pertb $VM_PARAMS_YAML -n 1 --perturbation --model $gmsim/Pre-processing/srf_generation/velocity_model_generation/config_files/graves_pitarka_2016_model_modified.csv

#chmod g+rwXs -R $OUT_DIR
#chgrp nesi00213 -R $OUT_DIR

end_time=`date +$runtime_fmt`
echo $end_time

timestamp=`date +%Y%m%d_%H%M%S`
#test before update
if [[ -f $OUT_DIR/$REL_NAME.pertb ]]; then
    #passed

    python $gmsim/workflow/scripts/cybershake/add_to_mgmt_queue.py $MGMT_DB_LOC/mgmt_db_queue $REL_NAME VM_PERT completed $SLURM_JOB_ID

    if [[ ! -d $CH_LOG_FFP ]]; then
        mkdir $CH_LOG_FFP
    fi

    # save meta data
    python $gmsim/workflow/metadata/log_metadata.py $CH_LOG_FFP VM_PERT cores=$SLURM_NTASKS start_time=$start_time end_time=$end_time
else
    #reformat $res to remove '\n'
    res=`echo $res | tr -d '\n'`
    python $gmsim/workflow/scripts/cybershake/add_to_mgmt_queue.py $MGMT_DB_LOC/mgmt_db_queue $REL_NAME VM_PERT failed $SLURM_JOB_ID --error "$res"
fi