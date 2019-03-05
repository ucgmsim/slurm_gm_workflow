#!/bin/bash
# script version: slurm
#
# must be run with sbatch calc_rrups_single [observedGroundMotionsDirectory] [managementDBLocation]

#SBATCH --job-name=calc_rrups_single
#SBATCH --account=nesi00213
#SBATCH --partition=nesi_research
#SBATCH --time=01:00:00
#SBATCH --cpus-per-task=40

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

REL_NAME=`basename REL`
REL_YAML="$REL/sim_params.yaml"

SRF_FILE=$(getFromYaml ${REL_YAML} srf_file)
STATION_FILE=$(getFromYaml ${REL_YAML} stat_file)
FD=$(getFromYaml ${REL_YAML} FD_STATLIST)

OUT_DIR=${REL}/IM_Calc

if [[ ! -f ${OUT_DIR}/rrup_${REL_NAME}.csv ]]
then
    echo ___calculating rrups___

    start_time=`date +${runtime_fmt}`
    echo "python $gmsim/workflow/scripts/management/update_mgmt_db.py $MGMT_DB_LOC rrup running --run_name $REL_NAME --j $SLURM_JOBID" >> ${MGMT_DB_LOC}/mgmt_db_queue/${timestamp}\_${SLURM_JOBID}

    time python ${IMPATH}/calculate_rrups.py -np ${SLURM_CPUS_PER_TASK} -o ${OUT_DIR}/rrup_${REL_NAME}.csv ${STATION_FILE} ${SRF_FILE} ${FD}
else
    echo "rrup file already present: ${OUT_DIR}/rrup_${REL_NAME}.csv"
    echo "Checking that there are enough rrups in it"
fi

if [[ -f ${OUT_DIR}/rrup_${REL_NAME}.csv ]]
then
    if [[ `wc -l ${OUT_DIR}/rrup_${REL_NAME}.csv` == `wc -l ${FD}` ]]
    then
        echo "python $gmsim/workflow/scripts/management/update_mgmt_db.py $MGMT_DB_LOC rrup completed --run_name $REL_NAME --force" >> ${MGMT_DB_LOC}/mgmt_db_queue/${timestamp}\_${SLURM_JOBID}
    else
        res="Not enough rrups for the station file"
    fi
else
    res="rrup file does not exist"
fi

if [[ -n ${res} ]]
then
    echo "python $gmsim/workflow/scripts/management/update_mgmt_db.py $MGMT_DB_LOC rrup failed --run_name $REL_NAME --error '$res' --force"  >> {MGMT_DB_LOC}/mgmt_db_queue/${timestamp}\_${SLURM_JOBID}
fi

date
