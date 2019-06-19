#!/usr/bin/env bash
# script version: slurm
#
# must be run with sbatch clean_up.sl [realisation directory] [realisation name] [management database location]

#SBATCH --job-name=im_plot
#SBATCH --account=nesi00213
#SBATCH --partition=prepost
#SBATCH --time=00:30:00
#SBATCH --cpus-per-task=36

if [[ ! -z ${CUR_ENV} && ${CUR_HPC} != "mahuika" ]]; then
    source $CUR_ENV/workflow/install_workflow/helper_functions/activate_env.sh $CUR_ENV "mahuika"
fi

CSV_PATH=$1
RRUP_OR_STATION_PATH=$2
OUTPUT_XYZ_DIR=$3

SRF_PATH=$4
MODEL_PARAMS=$5
OUTPUT_PLOT_DIR=$6

MGMT_DB_LOC=$7
$SRF_NAME=$8

script_start=`date`
echo "script started running at: $script_start"
runtime_fmt="%Y-%m-%d_%H:%M:%S"
timestamp=`date +%Y%m%d_%H%M%S`

start_time=`date +${runtime_fmt}`
echo ___im plot___

python $gmsim/workflow/scripts/cybershake/add_to_mgmt_queue.py $MGMT_DB_LOC/mgmt_db_queue $SRF_NAME im_plot running
res=`python $gmsim/visualization/im_plotting/im_plot.py $CSV_PATH $RRUP_OR_STATION_PATH --output $OUTPUT_XYZ_DIR; for f in $OUTPUT_XYZ_DIR/*;do python $gmsim/visualization/gmt/plot_ts.py $f --srf --out_dir $OUTPUT_PLOT_DIR;done`
exit_val=$?


end_time=`date +$runtime_fmt`
echo $end_time

if [[ $exit_val == 0 ]]; then
    #passed
    python $gmsim/workflow/scripts/cybershake/add_to_mgmt_queue.py $MGMT_DB_LOC/mgmt_db_queue $SRF_NAME im_plot completed
else
    python $gmsim/workflow/scripts/cybershake/add_to_mgmt_queue.py $MGMT_DB_LOC/mgmt_db_queue $SRF_NAME im_plot failed --error "$res"
fi

