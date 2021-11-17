#!/bin/bash
# script version: slurm
#
# must be run with sbatch hf2bb.sl [realisationDirectory] [managementDBLocation]

#SBATCH --job-name=hf2bb
#SBATCH --time=00:15:00
#SBATCH --cpus-per-task=1

if [[ ! -z ${CUR_ENV} && ${CUR_HPC} != "mahuika" ]]; then
    source $CUR_ENV/workflow/environments/helper_functions/activate_env.sh $CUR_ENV "mahuika"
fi

REL_LOC=$1
MGMT_DB_LOC=$2

shift 2
REM_ARGS="$@"

REL_NAME=`basename $REL_LOC`

HF_LOC=$REL_LOC/HF/Acc/HF.bin
BB_LOC=$REL_LOC/BB/Acc/BB.bin

if [[ ! -d $REL_LOC/BB/Acc ]]; then
    mkdir -p $REL_LOC/BB/Acc
fi

#updating the stats in managementDB
if [[ ! -d $MGMT_DB_LOC/mgmt_db_queue ]]; then
    #create the queue folder if not exist
    mkdir $MGMT_DB_LOC/mgmt_db_queue
fi
timestamp=`date +%Y%m%d_%H%M%S`
python $gmsim/workflow/automation/execution_scripts/add_to_mgmt_queue.py $MGMT_DB_LOC/mgmt_db_queue $REL_NAME HF2BB running $SLURM_JOB_ID

runtime_fmt="%Y-%m-%d_%H:%M:%S"
start_time=`date +$runtime_fmt`
echo $start_time

echo "python $gmsim/workflow/calculation/hf2bb.py $HF_LOC $BB_LOC"
python $gmsim/workflow/calculation/hf2bb.py $HF_LOC $BB_LOC $REM_ARGS

end_time=`date +$runtime_fmt`
echo $end_time

timestamp=`date +%Y%m%d_%H%M%S`
#test before update
res=`$gmsim/workflow/scripts/test_bb.sh $REL_LOC `
if [[ $? == 0 ]]; then
    #passed
    python $gmsim/workflow/automation/execution_scripts/add_to_mgmt_queue.py $MGMT_DB_LOC/mgmt_db_queue $REL_NAME HF2BB completed $SLURM_JOB_ID

    if [[ ! -d $REL_LOC/ch_log ]]; then
        mkdir $REL_LOC/ch_log
    fi
    fd_name=`python -c "from qcore import utils; pb = utils.load_sim_params('$REL_LOC/sim_params.yaml'); print(pb['FD_STATLIST'])"`
    fd_count=`cat $fd_name | wc -l`
    
    # save meta data
    python $gmsim/workflow/automation/metadata/log_metadata.py $REL_LOC HF2BB cores=$SLURM_NTASKS fd_count=$fd_count start_time=$start_time end_time=$end_time
else
    #reformat $res to remove '\n'
    res=`echo $res | tr -d '\n'`
    python $gmsim/workflow/automation/execution_scripts/add_to_mgmt_queue.py $MGMT_DB_LOC/mgmt_db_queue $REL_NAME HF2BB failed $SLURM_JOB_ID --error "$res"
fi
