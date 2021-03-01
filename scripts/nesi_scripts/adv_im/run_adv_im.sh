#!/bin/bash
# A script that prepare and run adv_im using previously ran validation runs
# arguments: 
#   1. the name of the adv_im model 
#   2. a file that contains a list of events to run (must match/subset of arg 3 and 4 below)
#   3. the path pointing to a simulation's root folder that contains (Runs, Data folder)
#   4. the path pointing to a directory containing observed data
# To avoid complexisity this script will only run one Adv_IM model at a time

# exit codes:
#    2: input errors
#    3: File/Directory exist error
#    5: failed to create screen socket
#   10: install_cybershake failed
#   11: failed to update root_params.yaml
#   12: failed to link BB
#   13: failed to link ObservedData
#   14: non-zero exit on test run

ADV_IM_NAME=${1:?please provide the name of a advanced IM model}
LIST_EVENTS_F=${2:?please provide a file that contains a list of events}
BBbin_root_dir=${3:?please provide the path to simulation root folder that contains BBbin}
OBS_DATA_DIR=${4:?please provide the path to ObservedData}

VALIDATION_VERSION=`basename $BBbin_root_dir`
validation_root_params_yaml=$BBbin_root_dir/Runs/root_params.yaml

# read gmsim version from source run. although its not being used by adv_im at the moment, the install script needs it
gmsim_version=`python -c "from qcore.utils import load_yaml,DotDictify; p=DotDictify(load_yaml(\"$validation_root_params_yaml\"));print(p.version)"`
if [[ $? != 0 ]];then
    echo "failed to read gmsim_version from $validation_root_params_yaml"
    exit 2
fi
gmsim_version_dir=$gmsim/workflow/templates/gmsim/$gmsim_version/
# a template config to run only adv_im
TASK_CONFIG_TEMPLATE=$gmsim/workflow/scripts/nesi_scripts/adv_im/task_config.yaml

SIM_DATA_DIR=$BBbin_root_dir/Data/

FAKE_STATION_LIST=/nesi/project/nesi00213/StationInfo/cantstations.ll #this list is used for install.py not actually used since BB has already ran
WCT_BENCHMARK_MINSEC=1500 # seconds used to run a ATC12 model on single waveform
WCT_BENCHMARK_MAXHOUR=6 # the wct for ATC12
BENCHMARK_OBS_SIZE=30 # the size for submitting obs
SPLIT_LINE='#####################################'
##### functions #####

# create screen socket and run automated workflow
create_runsocket () {
    screen -d -m -S run_$ADV_IM_NAME bash -c "python $gmsim/workflow/scripts/cybershake/run_cybershake.py $root_dir $USERNAME $TASK_CONFIG --n_max_retries 1"
}

# ask user for yes/no
ask_yn () {
    # $1: msg to display
    # $2: action/command to run if Yes
    # $3: action/command to run if No
    msg=${1}
    yes_action=${2}
    no_action=${3}
    echo $msg 'choose 1 or 2'
    select yn in "Yes" "No"; do
    case $yn in
        Yes )
            eval $yes_action
            break;;
        No )
            eval $no_action
            break;;
    esac
done
}

#############  some tests before running anything ###############
# quick test to see if model exist
ADV_IM_MODEL_DIR=$gmsim/IM_calculation/IM_calculation/Advanced_IM/Models/$ADV_IM_NAME
if [[ ! -d $ADV_IM_MODEL_DIR ]];then
    echo "directory for adv_im model: $ADV_IM_NAME does not exits"
    exit 2
fi
# check if BBbin_root_dir exist
if [[ ! -d $BBbin_root_dir ]];then
    echo "directory for BBbin_root_dir: $BBbin_root_dir does not exits"
    exit 2
fi

# check if OBS_DATA_DIR exist
if [[ ! -d $OBS_DATA_DIR ]];then
    echo "directory for OBS_DATA_DIR: $OBS_DATA_DIR does not exits"
    exit 2
fi

if [[ ! -f $ADV_IM_MODEL_DIR/run.py ]];then
    echo "run.py for $ADV_IM_NAME cannot be found at $ADV_IM_MODEL_DIR"
    exit 2
fi


##### checks that can use default values that wont affect adv_im runs #####
STATION_LIST=`python -c "from qcore.utils import load_yaml,DotDictify; p=DotDictify(load_yaml(\"$validation_root_params_yaml\"));print(p.stat_file)"`

if [[ ! -f $STATION_LIST ]];then
    echo "station list for validation run cannot be found on $STATION_LIST"
    ask_yn "use a fake station list?" 'STATION_LIST=$FAKE_STATION_LIST' 'exit 2'
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

#################################################################
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

# link Data folder
echo $SPLIT_LINE
echo "linking Data folder"
ln -s $SIM_DATA_DIR $root_dir

################################################################
#   determine mag catagory
################################################################
max_mag=`grep mag $root_dir/Data/VMs/*/vm_params.yaml | awk '{print $2}' | sort -unr | head -n 1`
if (( $(echo "$max_mag > 7.0" | bc -l) ));then
    mag_category=large
elif (( $(echo "$max_mag > 5.0" | bc -l) ));then
    mag_category=moderate
else
    mag_category=small
fi

# install the simulation
echo $SPLIT_LINE
echo "installing simulation folder"
echo $SPLIT_LINE
python $gmsim/workflow/scripts/cybershake/install_cybershake.py $root_dir $LIST_EVENTS_F $gmsim_version --stat_file_path $STATION_LIST
if [[ $? != 0 ]];then
    exit 10
fi
echo $SPLIT_LINE

################################################################
# add/update the adv_im models in root_params.yaml
python $gmsim/workflow/scripts/update_root_yaml.py $root_dir/Runs/root_params.yaml $ADV_IM_NAME

if [[ $? != 0 ]];then
    echo "failed to update root_params.yaml: $root_dir/Runs/root_params.yaml"
    exit 11
fi
################################################################

# link BB
echo $SPLIT_LINE
echo "linking BB"
bash $gmsim/workflow/scripts/link_bb.sh $BBbin_root_dir $root_dir $LIST_EVENTS_F

if [[ $? != 0 ]];then
    echo "failed to link BBs from $BBbin_root_dir to $root_dir"
    exit 12
fi

# link observed data
echo $SPLIT_LINE
echo "linking ObservedData"
bash $gmsim/workflow/scripts/link_obs.sh $OBS_DATA_DIR $root_dir $LIST_EVENTS_F

if [[ $? != 0 ]];then
    exit 13
fi

# cp task_config.yaml
if [[ -f $TASK_CONFIG ]];then
    rm $TASK_CONFIG
fi
cp $TASK_CONFIG_TEMPLATE $TASK_CONFIG

# run a test run on one waveform to get the time ratio for estimating WCT
# baseline number were using ATC12_story
echo $SPLIT_LINE
echo "Running test station to estimate WCT"
echo "using waveform from $mag_category event"
tmp_test_dir=$root_dir/test_tmp
if [[ ! -d $tmp_test_dir ]]; then
    mkdir -p $tmp_test_dir
fi
TIMEFORMAT='%R'
used_time="$(time (bash $gmsim/test_station/test_runpy.sh $ADV_IM_NAME $mag_category $tmp_test_dir) 2>&1 1>/dev/null)"
if [[ $? != 0 ]];then
    exit 14
fi
echo "used time: $used_time"
rm -r $tmp_test_dir

est_ratio=` echo $used_time / $WCT_BENCHMARK_MINSEC | bc -l`
echo "est_ratio = $est_ratio"
if (( $( echo "$est_ratio > 1" | bc -l ) ));then
    # up-scale the WCT
    sed -i "/threshold_time = /c\    threshold_time = $WCT_BENCHMARK_MAXHOUR*$est_ratio" $gmsim/workflow/estimation/estimate_wct.py
else
    sed -i "/threshold_time = /c\    threshold_time = $WCT_BENCHMARK_MAXHOUR" $gmsim/workflow/estimation/estimate_wct.py
fi


create_runsocket
while [[ $? != 0 ]]; 
do
    echo "failed to start up screen socket for automated submission"
    ask_yn "re-attempt to create screen in 5 seconds?" 'sleep 5;eval create_runsocketi;' 'exit 5;'
done

###### Observed related steps ######

# split list into portions base on ratio of runtime (compared to ATC12)
batch_size=`echo $BENCHMARK_OBS_SIZE / $est_ratio | bc`
bash $gmsim/workflow/scripts/split_list.sh $LIST_EVENTS_F $batch_size $obs_input_dir

if [[ $? == 0 ]];then
    # submit each list
    for obs_list in $obs_input_dir/*;
    do
        sbatch --job-name=`basename $obs_list` $gmsim/workflow/scripts/nesi_scripts/run_adv_im_obs_maui.sl $obs_linked_folder $obs_list 
    done
fi
