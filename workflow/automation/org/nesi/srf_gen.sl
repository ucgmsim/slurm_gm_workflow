#!/bin/bash
# script version: slurm
#
# must be run with sbatch srf_gen.sl [REL_FILEPATH] [MGMT_DB_LOC] [REL_NAME]

##SBATCH --partition=milan
#SBATCH --job-name=SRF_GEN
#SBATCH --time=01:00:00
#SBATCH --cpus-per-task=1



if [[ -n ${CUR_ENV} && ${CUR_HPC} != "mahuika" ]]; then
    source $CUR_ENV/workflow/workflow/environments/helper_functions/activate_env.sh $CUR_ENV "mahuika"
fi

module load LegacySystemLibs/7

REL_FILEPATH=${1:?REL_FILEPATH argument missing}
MGMT_DB_LOC=${2:?MGMT_DB_LOC argument missing}
REL_NAME=${3:?REL_NAME argument missing}

FAULT=$(echo $REL_NAME | cut -d"_" -f1)
SRF_DIR=`dirname $REL_FILEPATH`
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



if [ "${REL_FILEPATH##*.}" == "csv" ]; then 
    echo python $gmsim/Pre-processing/srf_generation/input_file_generation/realisation_to_srf.py $REL_FILEPATH
    python $gmsim/Pre-processing/srf_generation/input_file_generation/realisation_to_srf.py $REL_FILEPATH
else
    echo python $gmsim/Pre-processing/srf_generation/input_file_generation/generate_type5_srf.py $REL_FILEPATH $SRF_DIR
    python $gmsim/Pre-processing/srf_generation/input_file_generation/generate_type5_srf.py $REL_FILEPATH $SRF_DIR
fi



end_time=`date +$runtime_fmt`
echo $end_time

INFO_PATH=${REL_FILEPATH%.*}.info
STOCH_PATH=${REL_FILEPATH%.*}.stoch
SIM_PARAMS_PATH=${REL_FILEPATH%.*}.yaml
# to avoid clobbering type5 simulations (that look like type5_REL_NAME.yaml)
# the following sed command strips the last occurence of "type5_" in the full params path.
# /path/to/type5_realisation/type5_REL01.yaml -> /path/to/type5_realisation/REL01.yaml
SIM_PARAMS_PATH=$(echo "$SIM_PARAMS_PATH" | sed -e 's/\(.*\)type5_/\1/')

#test non-empty info file exists before update
res=`[[ -s $INFO_PATH ]]`
pass=$?

if [[ $pass == 0 ]]; then
    #passed

    chmod g+rwXs -R $SRF_DIR/$REL_NAME*
    chgrp nesi00213 -R $SRF_DIR/$REL_NAME*

    mkdir -p $SRF_DIR/../Stoch
    mkdir -p $SRF_DIR/../Sim_params
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
