#!/bin/bash
# script version: slurm
#
# must be run with sbatch calc_rrups_single [observedGroundMotionsDirectory] [managementDBLocation]

#SBATCH --job-name=calc_rrups_single
#SBATCH --account=nesi00213
#SBATCH --partition=prepost
#SBATCH --time=00:10:00
#SBATCH --cpus-per-task=12

if [[ ! -z ${CUR_ENV} && ${CUR_HPC} != "mahuika" ]]; then
    source $CUR_ENV/workflow/install_workflow/helper_functions/activate_env.sh $CUR_ENV "mahuika"
fi

function getFromYaml {
    echo $(python -c "from qcore.utils import load_sim_params; print(load_sim_params('$1').$2)")
}
export IMPATH=${gmsim}/IM_calculation/IM_calculation/scripts
export PYTHONPATH=$gmsim/qcore:/${PYTHONPATH}:${IMPATH}

script_start=`date`
echo "script started running at: $script_start"
runtime_fmt="%Y-%m-%d_%H:%M:%S"
timestamp=`date +%Y%m%d_%H%M%S`

REL=$1
MGMT_DB_LOC=$2

REL_NAME=`basename $REL`
REL_YAML="$REL/sim_params.yaml"

SRF_FILE=$(getFromYaml ${REL_YAML} srf_file)
# Get median srf file
SRF_FILE=${SRF_FILE//_REL??/}
STATION_FILE=$(getFromYaml ${REL_YAML} stat_file)
FD=$(getFromYaml ${REL_YAML} FD_STATLIST)

OUT_DIR=${REL}/verification
OUT_FILE=${OUT_DIR}/rrup_${REL_NAME//_REL??/}.csv

if [[ ! -f ${OUT_FILE} ]]
then
    # Create the output folder if needed
    mkdir -p $OUT_DIR
    echo ___calculating rrups___

    start_time=`date +${runtime_fmt}`
    python $gmsim/workflow/scripts/cybershake/add_to_mgmt_queue.py $MGMT_DB_LOC/mgmt_db_queue $REL_NAME rrup running $SLURM_JOB_ID

    time python ${IMPATH}/calculate_rrups_single.py -fd ${FD} -o ${OUT_FILE} ${STATION_FILE} ${SRF_FILE}
else
    echo "rrup file already present: ${OUT_FILE}"
    echo "Checking that there are enough rrups in it"
fi

if [[ -f ${OUT_DIR}/rrup_${REL_NAME}.csv ]]
then
    if [[ $(wc -l < ${OUT_FILE}) == $(( $(wc -l < ${FD}) + 1)) ]]
    then
        python $gmsim/workflow/scripts/cybershake/add_to_mgmt_queue.py $MGMT_DB_LOC/mgmt_db_queue $REL_NAME rrup completed $SLURM_JOB_ID
    else
        res="Not enough rrups for the station file"
    fi
else
    res="rrup file does not exist"
fi

if [[ -n ${res} ]]
then
    python $gmsim/workflow/scripts/cybershake/add_to_mgmt_queue.py $MGMT_DB_LOC/mgmt_db_queue $REL_NAME rrup failed $SLURM_JOB_ID --error '$res'
fi

date
