#!/bin/bash
# removes restart files from tar files in a full simulation run


if [[ $# -lt 2 ]]; then
    echo "please provide the path to 1. Runs 2. the list of faults to run (without r count)"
    exit 1
fi

run_dir=$1
list=$2


for fault in `cat $list | awk '{print $1}'`;
do
    echo $fault
    #get the LF.tar
    fault_dir=$run_dir/$fault

    #loop through rels
    
    for rel_dir in $fault_dir/*/;
    do
        lf_tar=$rel_dir/LF.tar
        if [ -f $lf_tar ];then
            echo "target file: $lf_tar"
            tmp_dir=$rel_dir/LF_temp
            if [ -d $tmp_dir ]; then
                rm -rf $tmp_dir
            fi
            #untar and remove if only successfully untar
            tar -C $rel_dir -xvf $lf_tar
            echo $rel_dir $tmp_dir
            res=$?
            if [[ $? == 0 ]]; then
                rm $lf_tar
                rm -r $tmp_dir/Restart
                #
                python -c "from scripts.clean_up import tar_files; tar_files('$tmp_dir', '$rel_dir/LF.tar')"
                rm -rf $tmp_dir
            fi
        fi    

    done
done
