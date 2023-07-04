#!/bin/bash
# script version: slurm
#
# must be run with sbatch vm_gen.sl [REL_YAML] [MGMT_DB_LOC] [REL_NAME]

#SBATCH --job-name=NHM_2_REL
#SBATCH --time=01:00:00
#SBATCH --cpus-per-task=32

if [[ -n ${CUR_ENV} && ${CUR_HPC} != "mahuika" ]]; then
    source $CUR_ENV/workflow/workflow/environments/helper_functions/activate_env.sh $CUR_ENV "mahuika"
fi

# REL_NAME should usually be the median/fault name
REL_NAME=${1:?REL_NAME argument missing}
N_RELS=${2:?N_RELS argument missing}
NHM_LOCATION=${3:?N_RELS argument missing}
VERSION=${4:?VERSION argument missing}
OUT_DIR=${5:?OUT_DIR argument missing}
MGMT_DB_LOC=${6:?MGMT_DB_LOC argument missing}

FAULT=$(echo $REL_NAME | cut -d"_" -f1)
SRF_DIR=`dirname $REL_YAML`

SIM_DIR=$MGMT_DB_LOC/Runs/$FAULT/$REL_NAME
CH_LOG_FFP=$SIM_DIR/ch_log


#updating the stats in managementDB
if [[ ! -d $MGMT_DB_LOC/mgmt_db_queue ]]; then
    #create the queue folder if not exist
    mkdir $MGMT_DB_LOC/mgmt_db_queue
fi
timestamp=`date +%Y%m%d_%H%M%S`
runtime_fmt="%Y-%m-%d_%H:%M:%S"
start_time=`date +$runtime_fmt`
echo $start_time

python $gmsim/workflow/workflow/automation/execution_scripts/add_to_mgmt_queue.py $MGMT_DB_LOC/mgmt_db_queue $REL_NAME SRF_GEN running $SLURM_JOB_ID --start_time "$start_time" --nodes $SLURM_NNODES --cores $SLURM_CPUS_PER_TASK --wct 01:00:00

echo python $gmsim/Pre-processing/srf_generation/source_parameter_generation/nhm_to_realisation.py $FAULT $N_RELS $NHM_LOCATION --output_dir $OUT_DIR --version $VERSION
python $gmsim/Pre-processing/srf_generation/input_file_generation/realisation_to_srf.py $REL_YAML

end_time=`date +$runtime_fmt`
echo $end_time

INFO_PATH=${REL_YAML%.*}.info
STOCH_PATH=${REL_YAML%.*}.stoch
SIM_PARAMS_PATH=${REL_YAML%.*}.yaml

#test non-empty info file exists before update
res=`[[ -s $INFO_PATH ]]`
pass=$?

if [[ $pass == 0 ]]; then
    #passed

    chmod g+rwXs -R $SRF_DIR/$REL_NAME*
    chgrp nesi00213 -R $SRF_DIR/$REL_NAME*

    mv $STOCH_PATH $SRF_DIR/../Stoch
    mv $SIM_PARAMS_PATH $SRF_DIR/../Sim_params

    python $gmsim/workflow/workflow/automation/execution_scripts/add_to_mgmt_queue.py $MGMT_DB_LOC/mgmt_db_queue $REL_NAME SRF_GEN completed $SLURM_JOB_ID --end_time "$end_time"

    if [[ ! -d $CH_LOG_FFP ]]; then
        mkdir $CH_LOG_FFP
    fi

    # save meta data
    python $gmsim/workflow/workflow/automation/metadata/log_metadata.py $SIM_DIR SRF_GEN cores=$SLURM_CPUS_PER_TASK start_time=$start_time end_time=$end_time
else
    #reformat $res to remove '\n'
    res=`echo $res | tr -d '\n'`
    python $gmsim/workflow/workflow/automation/execution_scripts/add_to_mgmt_queue.py $MGMT_DB_LOC/mgmt_db_queue $REL_NAME SRF_GEN failed $SLURM_JOB_ID --error "$res" --end_time "$end_time"
fi
