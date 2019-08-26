#!/usr/bin/env bash
# script version: slurm
#
# must be run with sbatch im_plot.sl [imcalc csv path] [station file path] [output xyz dir] [srf path] [model params path] [realisation name] [management database location]

#SBATCH --job-name=im_plot
#SBATCH --account=nesi00213
#SBATCH --partition=prepost
#SBATCH --time=00:30:00
#SBATCH --cpus-per-task=4

if [[ ! -z ${CUR_ENV} && ${CUR_HPC} != "mahuika" ]]; then
    source $CUR_ENV/workflow/install_workflow/helper_functions/activate_env.sh $CUR_ENV "mahuika"
fi

CSV_PATH=$1
STATION_FILE_PATH=$2
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

python $gmsim/workflow/scripts/cybershake/add_to_mgmt_queue.py $MGMT_DB_LOC/mgmt_db_queue $SRF_NAME IM_plot running $SLURM_JOB_ID
res=`python $gmsim/visualization/visualization/im_plotting/im_plot.py $CSV_PATH $STATION_FILE_PATH --output $OUTPUT_XYZ_DIR`

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

plot_stations () {
    if [[ -f "$f" ]]; then
        out_dir="${f//./_}_png_stations"
        echo "outputtig pngs to $out_dir"
        python $gmsim/visualization/visualization/gmt/plot_stations.py $f -n 4 --srf "$SRF_PATH" --model_params $MODEL_PARAMS --out_dir $out_dir
    fi
}

if [[ $exit_val == 0 ]]; then
    echo "finished making csvs for im_plot"
    
    echo "loading python2 mahuika environment"    
    load_python2_mahuika
    
    echo "start plotting stations"
    
    failed=0
    fail_msgs=()
    success_msgs=()    
    for f in $OUTPUT_XYZ_DIR/*; do
        res2=`plot_stations`
        exit_val2=$?
        if [[ $exit_val2 != 0 ]]; then
            failed=1
            failed_msgs+=( "failed $res2" )
        else
            success_msgs+=( "successfully $res2" )
        fi 
    done
    
    ## Reset to python3 virtual environment, otherwise add_to_mgmt_queue would not work
    source $CUR_ENV/workflow/install_workflow/helper_functions/activate_env.sh $CUR_ENV "mahuika"
    
    echo "Resetted to python3, cur_env is $CUR_ENV"

    if [[ $failed == 0 ]]; then
        ## log information about params used to .out file    
        echo "srf_file $SRF_PATH"
        echo "model_params $MODEL_PARAMS"
        printf '%s\n' "${success_msgs[@]}" 
        python $gmsim/workflow/scripts/cybershake/add_to_mgmt_queue.py $MGMT_DB_LOC/mgmt_db_queue $SRF_NAME IM_plot completed $SLURM_JOB_ID
    else
       printf '%s\n' "${failed_msgs[@]}" 
       python $gmsim/workflow/scripts/cybershake/add_to_mgmt_queue.py $MGMT_DB_LOC/mgmt_db_queue $SRF_NAME IM_plot failed $SLURM_JOB_ID --error "$failed_msgs"
    fi
else
    python $gmsim/workflow/scripts/cybershake/add_to_mgmt_queue.py $MGMT_DB_LOC/mgmt_db_queue $SRF_NAME IM_plot failed $SLURM_JOB_ID --error "$res"
fi

end_time=`date +$runtime_fmt`
echo $end_time
