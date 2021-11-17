#!/bin/bash
# script version: slurm
#
# must be run with sbatch calc_rrups_single [observedGroundMotionsDirectory] [managementDBLocation]

#SBATCH --job-name=calc_rrups_single
#SBATCH --time=00:10:00
#SBATCH --cpus-per-task=12

if [[ ! -z ${CUR_ENV} && ${CUR_HPC} != "mahuika" ]]; then
    source $CUR_ENV/workflow/environments/helper_functions/activate_env.sh $CUR_ENV "mahuika"
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
REL_YAML=$(python -c "from qcore.simulation_structure import get_sim_params_yaml_path; print(get_sim_params_yaml_path('${REL}'))")

SRF_FILE=$(getFromYaml ${REL_YAML} srf_file)
# Get median srf file
SRF_FILE=${SRF_FILE//_REL??/}
STATION_FILE=$(getFromYaml ${REL_YAML} stat_file)
FD=$(getFromYaml ${REL_YAML} FD_STATLIST)

OUT_FILE=$(python -c "from qcore.simulation_structure import get_rrup_path; print(get_rrup_path('${MGMT_DB_LOC}', '${REL}'))")
OUT_DIR=`dirname $OUT_FILE`
mkdir -p $OUT_DIR

if [[ ! -f ${OUT_FILE} ]]
then
    # Create the output folder if needed
    echo ___calculating rrups___

    start_time=`date +${runtime_fmt}`
    python $gmsim/workflow/automation/execution_scripts/add_to_mgmt_queue.py $MGMT_DB_LOC/mgmt_db_queue $REL_NAME rrup running $SLURM_JOB_ID

    time python ${IMPATH}/calculate_rrups_single.py -fd ${FD} -o ${OUT_FILE} ${STATION_FILE} ${SRF_FILE}
else
    echo "rrup file already present: ${OUT_FILE}"
    echo "Checking that there are enough rrups in it"
fi

if [[ -f ${OUT_FILE} ]]
then
    if [[ $(wc -l < ${OUT_FILE}) == $(( $(wc -l < ${FD}) + 1)) ]]
    then
        python $gmsim/workflow/automation/execution_scripts/add_to_mgmt_queue.py $MGMT_DB_LOC/mgmt_db_queue $REL_NAME rrup completed $SLURM_JOB_ID
    else
        res="Not enough rrups for the station file"
    fi
else
    res="rrup file does not exist"
fi

if [[ -n ${res} ]]
then
    python $gmsim/workflow/automation/execution_scripts/add_to_mgmt_queue.py $MGMT_DB_LOC/mgmt_db_queue $REL_NAME rrup failed $SLURM_JOB_ID --error $res
fi

date
