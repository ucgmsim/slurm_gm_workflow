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

MGMT_DB_LOC=$6
SRF_NAME=$7

script_start=`date`
echo "script started running at: $script_start"
runtime_fmt="%Y-%m-%d_%H:%M:%S"
timestamp=`date +%Y%m%d_%H%M%S`

start_time=`date +${runtime_fmt}`
echo ___im plot___

python $gmsim/workflow/scripts/cybershake/add_to_mgmt_queue.py $MGMT_DB_LOC/mgmt_db_queue $SRF_NAME IM_plot running
res=`python $gmsim/visualization/im_plotting/im_plot.py $CSV_PATH $RRUP_OR_STATION_PATH --output $OUTPUT_XYZ_DIR`
module load Python/2.7.14-gimkl-2017a

## Reset the PYTHONPATH
export PYTHONPATH=''

## PYTHONPATH (this can be removed once qcore is installed as a pip package)
export PYTHONPATH=/nesi/project/nesi00213/opt/mahuika/qcore:$PYTHONPATH

## PYTHONPATH for workflow
export PYTHONPATH=/nesi/project/nesi00213/workflow:$PYTHONPATH

## Load the python2 virtual environment
source /nesi/project/nesi00213/share/virt_envs/python2_mahuika/bin/activate

res2=`for f in $OUTPUT_XYZ_DIR/*; do if [ -f "$f" ]; then echo "ploting $f"; $gmsim/visualization/gmt/plot_stations.py $f --srf $SRF_PATH --model_params $MODEL_PARAMS --out_dir "${f//./_}_png_stations"; echo "output pngs save to ${f//./_}_png_stations"; fi; done`

echo "srf_file $SRF_PATH"
echo "model_params $MODEL_PARAMS"
echo "$res2"

exit_val=$?


end_time=`date +$runtime_fmt`
echo $end_time

## Reset to python3 virtual environment
source $CUR_ENV/workflow/install_workflow/helper_functions/activate_env.sh $CUR_ENV "mahuika"

if [[ $exit_val == 0 ]]; then
    #passed
    python $gmsim/workflow/scripts/cybershake/add_to_mgmt_queue.py $MGMT_DB_LOC/mgmt_db_queue $SRF_NAME IM_plot completed
else
    python $gmsim/workflow/scripts/cybershake/add_to_mgmt_queue.py $MGMT_DB_LOC/mgmt_db_queue $SRF_NAME IM_plot failed --error "$res $res2"
fi
