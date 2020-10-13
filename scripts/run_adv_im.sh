#!/bin/bash

# To avoid complexisity this script will only run one Adv_IM model at a time
if [[ $# -lt 2 ]];then
    # more than one args are provided, exit and warn user
    #echo "this script only runs on one Adv_IM model, please make sure only one is provided"
    echo "please provide 1. model name 2. list of events"
    exit 1
fi
ADV_IM_NAME=$1
LIST_EVENTS_F=$2

# some constant variables, only update these when version changes
VALIDATION_VERSION=v20p5p8
BBbin_root_dir=/nesi/nobackup/nesi00213/RunFolder/ykh22/Adv_IM/$VALIDATION_VERSION
SIM_DATA_DIR=/nesi/nobackup/nesi00213/RunFolder/ykh22/Adv_IM/$VALIDATION_VERSION/Data/
TASK_CONFIG_TEMPLATE=$BBbin_root_dir/task_config.yaml
OBS_DATA_DIR=/nesi/nobackup/nesi00213/RunFolder/ykh22/Adv_IM/ObservedGroundMotions
FAKE_STATION_LIST=/nesi/project/nesi00213/StationInfo/cantstations.ll #this list is used for install.py not actually used since BB has already ran
WCT_BENCHMARK_MINSEC=1477 # seconds used to run a ATC12 model on single waveform
WCT_BENCHMARK_MAXHOUR=6 # the wct for ATC12
BENCHMARK_OBS_SIZE=30 # the size for submitting obs
SPLIT_LINE='#####################################'

#############  some tests before running anything ###############
# quick test to see if model exist
ADV_IM_MODEL_DIR=$gmsim/IM_calculation/IM_calculation/Advanced_IM/Models/$ADV_IM_NAME
if [[ ! -d $ADV_IM_MODEL_DIR ]];then
    echo "directory for $ADV_IM_NAME does not exits"
    exit 2
fi

if [[ ! -f $ADV_IM_MODEL_DIR/run.py ]];then
    echo "run.py for $ADV_IM_NAME cannot be found at $ADV_IM_MODEL_DIR"
    exit 2
fi


USERNAME=`whoami`
root_dir=$nobackup/RunFolder/$USERNAME/$VALIDATION_VERSION'_'$ADV_IM_NAME
TASK_CONFIG=$root_dir/task_config.yaml
# echo $root_dir
# test if folder name exist
# if exist, exit
if [[ -e $root_dir ]];then
    echo "$root_dir already exist, please delete the old simulation or rename the folder"
    exit 3
else
    mkdir -p $root_dir
fi

# OBS related folders

obs_linked_folder=$root_dir/ObservedGroundMotions
if [[ -e $obs_linked_folder ]];then
    echo "$obs_linked_folder exist, please remove/rename the old folder"
    exit 3
fi

obs_input_dir=$root_dir/obs_input_tmp
if [[ -e $obs_input_dir ]];then
    echo "$obs_input_dir exist, please remove/rename the old folder"
    exit 3
else
    mkdir -p $root_dir/obs_input_tmp
fi

################################################################
# creates a root_faults.yaml using sed to replace ADV_IM_MODELS
gmsim_version=18.5.3.1.a
gmsim_version_dir=$gmsim/workflow/templates/gmsim/$gmsim_version/

template_name=root_defaults.yaml_template

sed "s/ADV_IM_MODELS/$ADV_IM_NAME/g" $gmsim_version_dir/$template_name > $gmsim_version_dir/root_defaults.yaml

# link Data folder
echo $SPLIT_LINE
echo "linking Data folder"
ln -s $SIM_DATA_DIR $root_dir

# install the simulation
echo $SPLIT_LINE
echo "installing simulation folder"
echo $SPLIT_LINE
python $gmsim/workflow/scripts/cybershake/install_cybershake.py $root_dir $LIST_EVENTS_F $gmsim_version --stat_file_path $FAKE_STATION_LIST
echo $SPLIT_LINE

if [[ $? != 0 ]];then
    exit 1
fi

# link BB
echo $SPLIT_LINE
echo "linking BB"
bash $gmsim/workflow/scripts/link_bb.sh $BBbin_root_dir $root_dir $LIST_EVENTS_F

# link observed data
echo $SPLIT_LINE
echo "linking ObservedData"
bash $gmsim/workflow/scripts/link_obs.sh $OBS_DATA_DIR $root_dir $LIST_EVENTS_F

# cp task_config.yaml
if [[ -f $TASK_CONFIG ]];then
    rm $TASK_CONFIG
fi
cp $TASK_CONFIG_TEMPLATE $TASK_CONFIG

# run a test run on HALS station to get the time ratio for estimating WCT
# comparing to ATC12_story
echo $SPLIT_LINE
echo "Running test station to estimate WCT"
tmp_test_dir=$root_dir/test_tmp
if [[ ! -d $tmp_test_dir ]]; then
    mkdir -p $tmp_test_dir
fi
TIMEFORMAT='%R'
used_time="$(time (bash $gmsim/test_station/test_runpy.sh $ADV_IM_NAME $tmp_test_dir) 2>&1 1>/dev/null)"
echo "used time: $used_time"
rm -r $tmp_test_dir

est_ratio=` echo $used_time / $WCT_BENCHMARK_MINSEC | bc -l`
echo "st_ration = $est_ratio"
if (( $( echo "$est_ratio > 1" | bc -l ) ));then
    # up-scale the WCT
    sed -i "/threshold_time = /c\    threshold_time = $WCT_BENCHMARK_MAXHOUR*$est_ratio" $gmsim/workflow/estimation/estimate_wct.py
else
    sed -i "/threshold_time = /c\    threshold_time = $WCT_BENCHMARK_MAXHOUR" $gmsim/workflow/estimation/estimate_wct.py
fi


# create screen socket and run automated workflow
screen -d -m -S run_$ADV_IM_NAME bash -c "python $gmsim/workflow/scripts/cybershake/run_cybershake.py $root_dir $USERNAME $TASK_CONFIG --n_max_retries 1"

# Observed related steps

# split list into portions base on ratio of runtime (compared to ATC12)
batch_size=`echo $BENCHMARK_OBS_SIZE / $est_ratio | bc`
bash $gmsim/workflow/scripts/split_list.sh $LIST_EVENTS_F $batch_size $obs_input_dir

if [[ $? == 0 ]];then
    # submit each list
    for obs_list in $obs_input_dir/*;
    do
        sbatch --job-name=`basename $obs_list` $gmsim/workflow/scripts/run_adv_im_obs_maui.sl $obs_linked_folder $obs_list 
    done
fi
