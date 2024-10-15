#!/bin/bash
# script version: slurm
#
# must be run with sbatch install_realisation.sl [REL_NAME] [SIMULATION_ROOT]

#SBATCH --job-name=install_rel
#SBATCH --time=00:15:00
#SBATCH --cpus-per-task=1

source $CUR_ENV/workflow/workflow/environments/helper_functions/activate_env.sh $CUR_ENV "mahuika"


REL_NAME=${1:?REL_NAME argument missing}
SIMULATION_ROOT=${2:?SIMULATION_ROOT argument missing}

FAULT=$(echo ${REL_NAME/_REL*/})
SIM_DIR=$SIMULATION_ROOT/Runs/$FAULT/$REL_NAME
CH_LOG_FFP=$SIM_DIR/ch_log


mkdir -p $SIM_DIR
mkdir -p $SIMULATION_ROOT/mgmt_db_queue

timestamp=`date +%Y%m%d_%H%M%S`
runtime_fmt="%Y-%m-%d_%H:%M:%S"
start_time=`date +$runtime_fmt`
echo $start_time

python $gmsim/workflow/workflow/automation/execution_scripts/add_to_mgmt_queue.py $SIMULATION_ROOT/mgmt_db_queue $REL_NAME INSTALL_REALISATION running $SLURM_JOB_ID --start_time "$start_time" --nodes $SLURM_NNODES --cores $SLURM_CPUS_PER_TASK --wct 00:15:00

CMD="python $gmsim/workflow/workflow/automation/install_scripts/install_realisation.py $SIMULATION_ROOT $REL_NAME"
echo "${CMD}"
$CMD

end_time=`date +$runtime_fmt`
echo $end_time

timestamp=`date +%Y%m%d_%H%M%S`
if [[ -f $SIM_DIR/sim_params.yaml ]] && [[ -s $SIM_DIR/sim_params.yaml ]]; then
    # File exists and is not empty. Passed

    python $gmsim/workflow/workflow/automation/execution_scripts/add_to_mgmt_queue.py $SIMULATION_ROOT/mgmt_db_queue $REL_NAME INSTALL_REALISATION completed $SLURM_JOB_ID --end_time "$end_time"

    mkdir -p $CH_LOG_FFP

    # save meta data
    python $gmsim/workflow/workflow/automation/metadata/log_metadata.py $SIM_DIR INSTALL_REALISATION cores=$SLURM_CPUS_PER_TASK start_time=$start_time end_time=$end_time
else
    #reformat $res to remove '\n'
    res=`echo $res | tr -d '\n'`
    python $gmsim/workflow/workflow/automation/execution_scripts/add_to_mgmt_queue.py $SIMULATION_ROOT/mgmt_db_queue $REL_NAME INSTALL_REALISATION failed $SLURM_JOB_ID --error "$res" --end_time "$end_time"
fi
