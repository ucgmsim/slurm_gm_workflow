#!/bin/bash

if [[ $# -lt  2 ]];then
    echo "please provide the list to run, 2. and output dir"
    exit 1
fi
trap "echo Exited!; exit;" SIGINT SIGTERM

bin_dir=/home/jonney/validate_im/visualization/
station=/home/nesi00213/StationInfo/non_uniform_whole_nz_with_real_stations-hh400_v18p6.ll

run_dir=$1
out_dir=$2

for run_name_fp in $run_dir/*;
do
    run_name=`basename $run_name_fp`
    mkdir $out_dir/$run_name
    cd $out_dir/$run_name
    mkdir $out_dir/$run_name/Srf
    rsync -avh kupe:/nesi/nobackup/nesi00213/RunFolder/Cybershake/v18p6/Data/Sources/$run_name/Srf/*HYP01* $out_dir/$run_name/Srf

    for i in `find $run_name_fp/IM_calc/*HYP01*/ -name "*.csv"`
    do
        echo $i
        realization=`basename $i`
        realization=${realization%.*}
        mkdir $out_dir/$run_name/$realization
        python $bin_dir/im_plotting/im_plot.py `realpath $i` $station -o $out_dir/$run_name/$realization/
        #done creating xyz
        cd $out_dir/$run_name/$realization
        for xyz in *.xyz;
        do
            echo "plotting $xyz"
            python $bin_dir/gmt/plot_stations.py $xyz --srf $out_dir/$run_name/Srf/*HYP01*.srf
            mv PNG_stations ${xyz%.xyz}
            echo "done"
        done
    done

done

#python /home/jonney/validate_im/visualization/im_plotting/im_plot.py /home/jonney/validate_im/IM_calc/AlpineF2K_HYP01-47_S1244/AlpineF2K_HYP01-47_S1244.csv /home/nesi00213/StationInfo/non_uniform_whole_nz_with_real_stations-hh400_v18p6.ll -o /home/jonney/validate_im/plot
