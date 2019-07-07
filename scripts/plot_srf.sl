#!/usr/bin/env bash
# script version: slurm
#
# must be run with sbatch plot_srf.sl [srf dir] [output folder] [management database location] [realization name]

#SBATCH --job-name=plot_srf
#SBATCH --account=nesi00213
#SBATCH --partition=prepost
#SBATCH --time=00:30:00
#SBATCH --cpus-per-task=1

if [[ ! -z ${CUR_ENV} && ${CUR_HPC} != "mahuika" ]]; then
    source $CUR_ENV/workflow/install_workflow/helper_functions/activate_env.sh $CUR_ENV "mahuika"
fi

SRF_DIR=$1
OUTPUT_DIR=$2
MGMT_DB_LOC=$3
SRF_NAME=$4

SRF_PATH="${SRF_DIR}/${SRF_NAME}.srf"
STATIC_OUTPUT_MAP_PLOT_PATH="${SRF_DIR}/${SRF_NAME}_map.png"

script_start=`date`
echo "script started running at: $script_start"
runtime_fmt="%Y-%m-%d_%H:%M:%S"
timestamp=`date +%Y%m%d_%H%M%S`

start_time=`date +${runtime_fmt}`
echo ___plotting SRF___

python $gmsim/workflow/scripts/cybershake/add_to_mgmt_queue.py $MGMT_DB_LOC/mgmt_db_queue $SRF_NAME plot_srf running
res=`python $gmsim/visualization/gmt/plot_srf_square.py "$SRF_PATH" --out-dir "$OUTPUT_DIR"`
exit_val=$?

load_python2_mahuika () {
    # Load python2, have to do this as virtualenv points to this python
    # verions, which is not accessible without loading
    module load Python/2.7.14-gimkl-2017a

    # Reset the PYTHONPATH
    export PYTHONPATH=''

    # PYTHONPATH (this can be removed once qcore is installed as a pip package)
    export PYTHONPATH=/nesi/project/nesi00213/opt/mahuika/qcore:$PYTHONPATH

    # PYTHONPATH for workflow
    export PYTHONPATH=/nesi/project/nesi00213/workflow:$PYTHONPATH

    # Load the virtual environment
    source /nesi/project/nesi00213/share/virt_envs/python2_mahuika/bin/activate
}

load_python2_mahuika

res2=`python $gmsim/visualization/gmt/plot_srf_map.py "$SRF_PATH" 300 "active_faults"`
exit_val2=$?

end_time=`date +$runtime_fmt`
echo $end_time

source $CUR_ENV/workflow/install_workflow/helper_functions/activate_env.sh $CUR_ENV "mahuika"

if [[ $exit_val == 0 ]] && [[ $exit_val2 == 0 ]]; then
    # passed
    # output map plot is defaultly saved to srf folder, move it to Verification folder
    if [[ -f "$STATIC_OUTPUT_MAP_PLOT_PATH" ]]; then
        echo "outputted plots to $OUTPUT_DIR"
        mv "$STATIC_OUTPUT_MAP_PLOT_PATH" "$OUTPUT_DIR"
    fi

    python $gmsim/workflow/scripts/cybershake/add_to_mgmt_queue.py $MGMT_DB_LOC/mgmt_db_queue $SRF_NAME plot_srf completed
else
    errors=""
    if [[ $exit_val != 0 ]]; then
        errors+=" failed executing plot_srf_square.py "
    fi
    if [[ $exit_val2 != 0 ]]; then
        errors+=" failed executing plot_srf_map.py "
    fi
    python $gmsim/workflow/scripts/cybershake/add_to_mgmt_queue.py $MGMT_DB_LOC/mgmt_db_queue $SRF_NAME plot_srf failed --error "$errors"
fi

