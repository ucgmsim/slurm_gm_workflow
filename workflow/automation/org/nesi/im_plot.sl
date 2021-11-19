#!/usr/bin/env bash
# script version: slurm
#
# must be run with sbatch im_plot.sl [imcalc csv path] [station file path] [output xyz dir] [srf path] [model params path] [realisation name] [management database location]

#SBATCH --job-name=im_plot
#SBATCH --time=00:30:00
#SBATCH --cpus-per-task=1

if [[ ! -z ${CUR_ENV} && ${CUR_HPC} != "mahuika" ]]; then
    source $CUR_ENV/workflow/environments/helper_functions/activate_env.sh $CUR_ENV "mahuika"
fi

CSV_PATH=`realpath $1`
STATION_FILE_PATH=`realpath $2`
OUTPUT_XYZ_PARENT_DIR=`realpath $3`
SRF_PATH=`realpath $4`
MODEL_PARAMS=`realpath $5`
MGMT_DB_LOC=`realpath $6`
SRF_NAME=$7

COMPS=($(cat $CSV_PATH|cut -d , -f 2 |tail -n+2 |sort|uniq|tr " " "\n")) # find what components are used and makes an array

script_start=`date`
echo "script started running at: $script_start"
runtime_fmt="%Y-%m-%d_%H:%M:%S"
timestamp=`date +%Y%m%d_%H%M%S`

start_time=`date +${runtime_fmt}`
echo ___im plot___

python $gmsim/workflow/workflow/automation/execution_scripts/add_to_mgmt_queue.py $MGMT_DB_LOC/mgmt_db_queue $SRF_NAME IM_plot running $SLURM_JOB_ID

# check rotd50 was found in .csv
if [[ " ${COMPS[*]} " == *" rotd50 "* ]]; 
then 
    comps_to_plot=(geom rotd50)
else 
    comps_to_plot=(geom)
fi

for comp in ${comps_to_plot[@]};
do 
    OUTPUT_XYZ_DIR=$OUTPUT_XYZ_PARENT_DIR/$comp
    res=`python $gmsim/visualization/im/spatialise_im.py $CSV_PATH $STATION_FILE_PATH --out_dir $OUTPUT_XYZ_DIR -c $comp`
    exit_val=$?

    if [[ $exit_val == 0 ]]; then
        echo "finished making csvs for im_plot"

        echo "start plotting stations"
        
        failed=0
        fail_msgs=()
        success_msgs=()
        for f in $OUTPUT_XYZ_DIR/*.xyz; do
            cd $OUTPUT_XYZ_DIR
            out_dir=$(basename $f | cut -d'.' -f1)
            mkdir -p $out_dir
            cd $out_dir
            if [[ "$f" == *"real"* ]]; then
            res2=$(python $gmsim/visualization/sources/plot_items.py -t $SRF_NAME --xyz $f -c "$SRF_PATH" --xyz-cpt hot --xyz-cpt-invert --xyz-transparency 30 --xyz-size 0.1 --xyz-cpt-labels `cat $OUTPUT_XYZ_DIR/im_order.txt` -n 2)
            else
            res2=$(python $gmsim/visualization/sources/plot_items.py -t $SRF_NAME --xyz $f -c "$SRF_PATH" --xyz-model-params $MODEL_PARAMS --xyz-grid --xyz-grid-search 12m --xyz-landmask --xyz-cpt hot --xyz-cpt-invert --xyz-grid-contours --xyz-transparency 30 --xyz-size 1k --xyz-cpt-labels `cat $OUTPUT_XYZ_DIR/im_order.txt` -n 2)
            fi
            exit_val2=$?
            if [[ $exit_val2 != 0 ]]; then
                failed=1
                failed_msgs+=( "failed $res2" )
            else
                success_msgs+=( "successfully $res2" )
            fi 
        done

        if [[ $failed == 0 ]]; then
            ## log information about params used to .out file    
            echo "srf_file $SRF_PATH"
            echo "model_params $MODEL_PARAMS"
            printf '%s\n' "${success_msgs[@]}" 
            python $gmsim/workflow/workflow/automation/execution_scripts/add_to_mgmt_queue.py $MGMT_DB_LOC/mgmt_db_queue $SRF_NAME IM_plot completed $SLURM_JOB_ID
        else
        printf '%s\n' "${failed_msgs[@]}" 
        python $gmsim/workflow/workflow/automation/execution_scripts/add_to_mgmt_queue.py $MGMT_DB_LOC/mgmt_db_queue $SRF_NAME IM_plot failed $SLURM_JOB_ID --error "$failed_msgs"
        fi
    else
        python $gmsim/workflow/workflow/automation/execution_scripts/add_to_mgmt_queue.py $MGMT_DB_LOC/mgmt_db_queue $SRF_NAME IM_plot failed $SLURM_JOB_ID --error "$res"
    fi
done

end_time=`date +$runtime_fmt`
echo $end_time
