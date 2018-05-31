#!/bin/bash

#get list of runs



if [[ $# -lt 2 ]]; then
    echo "please provide a path to runs and the list of vms"
    exit 1
fi

root_dir=$1

if [ -d '$root_dir' ]; then
    echo "directory not exsit"
    exit 1
fi

list_runs=`cat $2`
#echo $run_dir
echo $list_runs

#get list of models in specific runs

for run_name in $list_runs;
do
    if [ -d $run_nam ];then
        run_dir=$root_dir/$run_name
        echo "run_dir: $run_dir"
        #copy the params to root dir
        #cp $run_dir/GM/Sim/Data/$run_name/params_base.py $run_dir
        #
        #getting model list
        #cd $run_dir/GM/Sim/Data/$run_name
        cd $run_dir/GM/Sim/Data/BB/Cant1D_v2-midQ_leer_hfnp2mm+_rvf0p8_sd50_k0p045
        echo cd $run_dir/GM/Sim/Data/BB/Cant1D_v2-midQ_leer_hfnp2mm+_rvf0p8_sd50_k0p045

        model_list=`ls $run_dir/GM/Sim/Data/BB/Cant1D_v2-midQ_leer_hfnp2mm+_rvf0p8_sd50_k0p045/ | cut -f1 -d'/'`

        for model in $model_list;
        do
            cd $run_dir
            #rm out.xyz
            #make the corner files
            if [ ! -d "$run_dir/GM/Sim/Data/$run_name/$model" ]; then
                python -c "import os; os.makedirs('$run_dir/GM/Sim/Data/$run_name/$model')"
            fi
            #exit 1
            python -c "from qcore.srf import srf2corners; srf2corners('$run_dir/Src/Model/$run_name/Srf/$model.srf','$run_dir/GM/Sim/Data/$run_name/$model/corners.txt')"
            #exit 1
            #generate out.xyz
            #echo python /home/nesi00213/post-processing/examples/export_IM_csv.py $run_dir/GM/Sim/Data/$run_name/fd_rt01-h0.400.ll $run_dir/GM/Sim/Data/BB/Cant1D_v2-midQ_leer_hfnp2mm+_rvf0p8_sd50_k0p045/$model/Vel out_$model.xyz plot_stations
            #python /home/nesi00213/post-processing/examples/export_IM_csv.py $run_dir/GM/Sim/Data/$run_name/fd_rt01-h0.400.ll $run_dir/GM/Sim/Data/BB/Cant1D_v2-midQ_leer_hfnp2mm+_rvf0p8_sd50_k0p045/$model/Vel out_$model.xyz plot_stations
            #plot out.xyz
            #plot_stations.py out_$model.xyz --srf  $run_dir/Src/Model/$run_name/Srf/$model.srf
            validation_folder=$run_dir/GM/Validation/$model
            for xyz in `find $validation_folder -name "*.xyz"`;
            do
                cd $validation_folder
                /nesi/projects/nesi00213/qcore/plot/plot_stations.py $xyz --srf_cnrs $run_dir/GM/Sim/Data/$run_name/$model/corners.txt --model_params $run_dir/VM/Model/$run_name/$run_name/model_params_rt01-h0.400
                mv PNG_stations ${xyz%.xyz}
                rm -rf GMT_WD_STATIONS
            done
            #/nesi/projects/nesi00213/qcore/plot/plot_stations.py out_$model.xyz --srf_cnrs $run_dir/GM/Sim/Data/$run_name/$model/corners.txt --model_params $run_dir/VM/Model/$run_name/$run_name/model_params_rt01-h0.400 #$run_dir/Src/Model/$run_name/Srf/$model.srf
            #exit 1
            #echo " mv $run_dir/GM/Sim/Figures/PNG_stations $run_dir/GM/Sim/Figures/PNG_stations_$model"
            #remove the old plots
            #if [[ -d $run_dir/GM/Sim/Figures/PNG_stations_$model ]];then
            #    rm -rf $run_dir/GM/Sim/Figures/PNG_stations_$model
            #fi
            #mv $run_dir/PNG_stations $run_dir/GM/Sim/Figures/PNG_stations_$model
            #rm -rf GMT_WD_STATIONS
            #exit
        done
        #params_dir=$run_dir/GM/Sim/Data/$run_name
        #get params_base to get path to corners ( to determind the srf model names), using pyhton
        #echo $model_list
        #python -c "import sys; sys.path.insert(0,'$params_dir'); import params_base as pb; print pb.srf_cnrs"
    fi
done
