#!/bin/bash

default_V1D=Cant1D_v2-midQ_leer_hfnp2mm+_rvf0p8_sd50_k0p045

#this script is to setup a download folder to download all list of BB in one command

#get the list of models

if [[ $# -lt 2 ]];then
    echo "pleave provide 1. the path to cybershake runs folder  2.the path to a file that contains a list of models to run, and 3. where to make the symbolic link"
    exit 1
fi

path_cybershake_runs=$1
file_list_model=$2
path_des=$3

for model in `cat $file_list_model`;
do
    ln -s $path_cybershake_runs/$model/BB $path_des/$model
done
