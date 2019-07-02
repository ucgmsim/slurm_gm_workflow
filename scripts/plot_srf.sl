#!/usr/bin/env bash
# script version: slurm
#
# must be run with sbatch plot_srf.sl [xyts file path] [srf file path] [output ts file path] [management database location] [realization name]

#SBATCH --job-name=plot_ts
#SBATCH --account=nesi00213
#SBATCH --partition=prepost
#SBATCH --time=00:30:00
#SBATCH --cpus-per-task=1

if [[ ! -z ${CUR_ENV} && ${CUR_HPC} != "mahuika" ]]; then
    source $CUR_ENV/workflow/install_workflow/helper_functions/activate_env.sh $CUR_ENV "mahuika"
fi

SRF_PATH=$1
OUTPUT_DIR=$2
MGMT_DB_LOC=$3
SRF_NAME=$4

script_start=`date`
echo "script started running at: $script_start"
runtime_fmt="%Y-%m-%d_%H:%M:%S"
timestamp=`date +%Y%m%d_%H%M%S`

start_time=`date +${runtime_fmt}`
echo ___plotting SRF___

python $gmsim/workflow/scripts/cybershake/add_to_mgmt_queue.py $MGMT_DB_LOC/mgmt_db_queue $SRF_NAME plot_srf running
res=`python $gmsim/visualization/gmt/plot_srf_square.py $SRF_PATH --out-dir "$OUTPUT_DIR/square_plot"`
exit_val=$?
res2=`python $gmsim/visualization/gmt/plot_srf_map.py $SRF_PATH 300 "active_faults"`
exit_val2=$?
echo "res$res res2$res2" >> "/home/melody.zhu/plot_srf_res.txt"

end_time=`date +$runtime_fmt`
echo $end_time


if [[ $exit_val == 0 && $exit_val2 == 0]]; then
    #passed
    python $gmsim/workflow/scripts/cybershake/add_to_mgmt_queue.py $MGMT_DB_LOC/mgmt_db_queue $SRF_NAME plot_srf completed
else
    errors=()
    if [[ $exit_val != 0 ]]; then
        errors+=( $res )
    fi
    if [[ $exit_val2 != 0 ]]; then
        errors+=( $res2 )
    fi
    echo "${erros[@]}"
    python $gmsim/workflow/scripts/cybershake/add_to_mgmt_queue.py $MGMT_DB_LOC/mgmt_db_queue $SRF_NAME plot_srf failed --error "${erros[@]}"
fi

