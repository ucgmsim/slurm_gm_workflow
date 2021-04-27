#!/bin/bash
# script version: slurm
# im_calc
#

# Please modify this file as needed, this is just a sample
#SBATCH --account=nesi00213
#SBATCH --ntasks=40
#SBATCH --time=24:00:00
#SBATCH --output im_calc_%j_%x.out
#SBATCH --error im_calc_%j_%x.err
#SBATCH --nodes=1
#SBATCH --hint=nomultithread
#SBATCH --exclusive

export IMPATH=$gmsim/IM_calculation/IM_calculation/scripts
export PYTHONPATH=$gmsim/qcore:/$PYTHONPATH:$IMPATH

if [[ $# -lt 2 ]];
then
    echo "please provide 1. path to obs_dir 2. list of event names"
    exit 1
fi

obs_dir=$1
list_event=$2
# use default binary if not given
opensees_bin=${3:-`python -c "from qcore.config import qconfig; print(qconfig['OpenSees'])"`}

for event in `cat $list_event | awk '{print $1}'`;
do 
    echo $event
    path_eventBB=$obs_dir/$event/*/*/accBB
    path_IM_calc=$obs_dir/IM_calc
    path_event_out=$path_IM_calc/$event
    # get station count
    station_count=`ls $path_eventBB | cut -d. -f1 | sort -u | wc -l`
    if [[ $station_count -le 0 ]]; then
        echo failed to get the station count in $path_eventBB
        exit 2
    fi
    # get module names used for simulation analysis
    root_params=`realpath $obs_dir/../Runs/root_params.yaml`
    if [[ $? == 0 ]] && [[ -f $root_params ]]; then
        adv_IM_models=`python -c "from qcore.utils import load_yaml; params=load_yaml('$root_params'); print(' '.join(params['advanced_IM']['models']));"`
    else
    #failed to find a model from config/yaml
        exit 2
    fi
    # run for all Models
    for adv_IM_model in $adv_IM_models;
    do
        # check for status
        # skip if completed
        res=`python $gmsim/workflow/scripts/verify_adv_IM.py $path_event_out $adv_IM_model`; res_return_code=$?

        # return code from verify_adv_IM is used to determine status.
        if [[ $res_return_code == 0 ]];then
            continue
        fi
        time python $IMPATH/calculate_ims.py $path_eventBB a -o $path_event_out -np 40 -i $event -r $event -t  o -e -a $adv_IM_model --OpenSees_path $opensees_bin
        # test for completion 
        res=`python $gmsim/workflow/scripts/verify_adv_IM.py $path_event_out $adv_IM_model`; res_return_code=$?
        # return code from verify_adv_IM is used to determine status.
        if [[ $res_return_code == 0 ]];then
            # completed
            echo $event >> $obs_dir/../list_done_$adv_IM_model
            find $path_event_out/*/$adv_IM_model/ -mindepth 1 -maxdepth 1 -type d -exec bash -c "f_path={}; relative_path=\${f_path#$path_event_out/}; tar --remove-files -uvf $path_event_out/${adv_IM_model}_out.tar -C $path_event_out \$relative_path" \;
            gzip $path_event_out/${adv_IM_model}_out.tar
        else
            echo "completion test failed after running $adv_IM_model on $path_event_out"
            echo "something went wrong, stopping the job, check logs for $path_event_out for $adv_IM_model"
            echo "$res"
            exit 3
        fi
    done
done
