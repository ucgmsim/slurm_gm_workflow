#!/bin/bash
# script version: slurm
#
# must be run with sbatch vm_pert.sl [VM_PARAMS_YAML] [OUT_DIR] [MGMT_DB_LOC] [REL_NAME]

#SBATCH --job-name=VM_PERT
#SBATCH --time=10:00:00
#SBATCH --cpus-per-task=4
export WCT=$(sacct -j $SLURM_JOB_ID -o timelimit -P -n)

if [[ -n ${CUR_ENV} && ${CUR_HPC} != "mahuika" ]]; then
    source $CUR_ENV/workflow/workflow/environments/helper_functions/activate_env.sh $CUR_ENV "mahuika"
fi

module load FFTW

VM_PARAMS_YAML=${1:?VM_PARAMS_YAML argument missing}
OUT_DIR=${2:?OUT_DIR argument missing}
MGMT_DB_LOC=${3:?MGMT_DB_LOC argument missing}
REL_NAME=${4:?REL_NAME argument missing}

FAULT=$(echo $REL_NAME | cut -d"_" -f1)
SIM_DIR=$MGMT_DB_LOC/Runs/$FAULT/$REL_NAME
CH_LOG_FFP=$SIM_DIR/ch_log


if [[ ! -d $OUT_DIR ]]; then
    mkdir -p $OUT_DIR
fi

#updating the stats in managementDB
if [[ ! -d $MGMT_DB_LOC/mgmt_db_queue ]]; then
    #create the queue folder if not exist
    mkdir $MGMT_DB_LOC/mgmt_db_queue
fi
timestamp=`date +%Y%m%d_%H%M%S`
runtime_fmt="%Y-%m-%d_%H:%M:%S"
start_time=`date +$runtime_fmt`
echo $start_time

python $gmsim/workflow/workflow/automation/execution_scripts/add_to_mgmt_queue.py $MGMT_DB_LOC/mgmt_db_queue $REL_NAME VM_PERT running $SLURM_JOB_ID --start_time "$start_time" --nodes $SLURM_NNODES --cores $SLURM_CPUS_PER_TASK --wct "$WCT"

# this step is run on Mahuika and depends on binaries tools from /scale_wlg_persistent/filesets/project/nesi00213/opt/mahuika/tools/GCC740
# (symlinked to /scale_wlg_persistent/filesets/project/nesi00213/opt/mahuika/hybrid_sim_tools)
# We have GCC920-built tools but we stick to old ones as random numbers will be different

module load GCC/7.4.0
if [ -f $OUT_DIR/$REL_NAME.pertb.csv ]; then
  echo time python $gmsim/Pre-processing/srf_generation/velocity_model_generation/generate_perturbation_file.py $OUT_DIR/$REL_NAME.pertb $VM_PARAMS_YAML -n 1 -v --perturbation --parameter_file $OUT_DIR/$REL_NAME.pertb.csv
  time python $gmsim/Pre-processing/srf_generation/velocity_model_generation/generate_perturbation_file.py $OUT_DIR/$REL_NAME.pertb $VM_PARAMS_YAML -n 1 -v --perturbation --parameter_file $OUT_DIR/$REL_NAME.pertb.csv
else
  echo time python $gmsim/Pre-processing/srf_generation/velocity_model_generation/generate_perturbation_file.py $OUT_DIR/$REL_NAME.pertb $VM_PARAMS_YAML -n 1 -v --perturbation --model $gmsim/Pre-processing/srf_generation/velocity_model_generation/config_files/graves_pitarka_2016_model_modified.csv
  time python $gmsim/Pre-processing/srf_generation/velocity_model_generation/generate_perturbation_file.py $OUT_DIR/$REL_NAME.pertb $VM_PARAMS_YAML -n 1 -v --perturbation --model $gmsim/Pre-processing/srf_generation/velocity_model_generation/config_files/graves_pitarka_2016_model_modified.csv
fi


end_time=`date +$runtime_fmt`
echo $end_time

timestamp=`date +%Y%m%d_%H%M%S`
#test before update
# Normal Python modules are built with GCC/9.2.0
module load GCC/9.2.0
res=`python $gmsim/qcore/qcore/validate_vm.py file $OUT_DIR/$REL_NAME.pertb $VM_PARAMS_YAML`
pass=$?

if [[ $pass == 0 ]]; then
    #passed - file is non-zero

    python $gmsim/workflow/workflow/automation/execution_scripts/add_to_mgmt_queue.py $MGMT_DB_LOC/mgmt_db_queue $REL_NAME VM_PERT completed $SLURM_JOB_ID --end_time "$end_time"

    python -c "from qcore import utils;d=utils.load('${SIM_DIR}/sim_params.yaml');d['emod3d']['model_style']=3;d['emod3d']['pertbfile']='$OUT_DIR/$REL_NAME.pertb';utils.dump_yaml(d,'${SIM_DIR}/sim_params.yaml')"

    if [[ ! -d $CH_LOG_FFP ]]; then
        mkdir $CH_LOG_FFP
    fi

    # save meta data
    python $gmsim/workflow/workflow/automation/metadata/log_metadata.py $SIM_DIR VM_PERT cores=$SLURM_CPUS_PER_TASK start_time=$start_time end_time=$end_time
else
    #reformat $res to remove '\n'
    res=`echo $res | tr -d '\n'`
    python $gmsim/workflow/workflow/automation/execution_scripts/add_to_mgmt_queue.py $MGMT_DB_LOC/mgmt_db_queue $REL_NAME VM_PERT failed $SLURM_JOB_ID --error "$res" --end_time "$end_time"
fi
