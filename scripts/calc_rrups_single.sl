#!/bin/bash
# script version: slurm
#
# must be run with sbatch calc_rrups_single [observedGroundMotionsDirectory] [managementDBLocation]

#SBATCH --job-name=calc_rrups_single
#SBATCH --account=nesi00213
#SBATCH --partition=large
#SBATCH --time=01:00:00
#SBATCH --cpus-per-task=12

function getFromYaml {
    echo $(python -c "from qcore.utils import load_sim_params; print(load_sim_params('$1').$2)")
}
export IMPATH=${gmsim}/IM_calculation
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
STATION_FILE=$(getFromYaml ${REL_YAML} stat_file)
FD=$(getFromYaml ${REL_YAML} FD_STATLIST)

OUT_DIR=${REL}/IM_Calc

if [[ ! -f ${OUT_DIR}/rrup_${REL_NAME}.csv ]]
then
    echo ___calculating rrups___

    start_time=`date +${runtime_fmt}`
    python $gmsim/workflow/scripts/cybershake/add_to_mgmt_queue.py $MGMT_DB_LOC/mgmt_db_queue $REL_NAME rrup running

    time python ${IMPATH}/calculate_rrups.py -fd ${FD} -o ${OUT_DIR}/rrup_${REL_NAME}.csv ${STATION_FILE} ${SRF_FILE}
else
    echo "rrup file already present: ${OUT_DIR}/rrup_${REL_NAME}.csv"
    echo "Checking that there are enough rrups in it"
fi

if [[ -f ${OUT_DIR}/rrup_${REL_NAME}.csv ]]
then
    if [[ $(wc -l < ${OUT_DIR}/rrup_${REL_NAME}.csv) == $(( $(wc -l < ${FD}) + 1)) ]]
    then
        python $gmsim/workflow/scripts/cybershake/add_to_mgmt_queue.py $MGMT_DB_LOC/mgmt_db_queue $REL_NAME rrup completed
    else
        res="Not enough rrups for the station file"
    fi
else
    res="rrup file does not exist"
fi

if [[ -n ${res} ]]
then
    python $gmsim/workflow/scripts/cybershake/add_to_mgmt_queue.py $MGMT_DB_LOC/mgmt_db_queue $REL_NAME rrup failed --error '$res'
fi

date
