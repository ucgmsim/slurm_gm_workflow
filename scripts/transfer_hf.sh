if [[ $# -lt 3 ]];then
    echo please provide the 1. list 2. source dir of Runs on hypocentre and 3. the des dir or Runs on Maui
    exit 1
fi

list_fault=$1
src_dir=$2
des_dir=$3

for fault in `cat $list_fault`;
do
    for rel in $src_dir/$fault/$fault*;
    do
        #check if HF is there
        hf_bin=$rel/HF/Acc/HF.bin
        if [[ -s $hf_bin ]];then
            rel_basename=`basename $rel`
            # copy
            echo rsync -avhmL $hf_bin $des_dir/$fault/$rel_basename/HF/Acc/
            #exit 1
            if [[ $? != 0 ]]; then
                echo something went wrong when scp files $hf_bin
                exit 2
            else
                # add to the mgmt_db_queue_tmp
                tmp_dir=$src_dir/../mgmt_queue_tmp
                if [[ ! -d $tmp_dir ]];then
                    mkdir $tmp_dir
                fi
                python $gmsim/workflow/scripts/cybershake/add_to_mgmt_queue.py $tmp_dir $rel_basename HF completed 
            fi
        else
         echo "something is wrong, $hf_bin is either not there or empty"
         exit 3
        fi
    done
done

#sync mgmt_db_queue_tmp
for tmp_cmd in $tmp_dir/*;
do
    rsync -avh $tmp_cmd $des_dir/../mgmt_db_queue/
    if [[ $? != 0 ]]; then
        echo something went wrong when transfering $tmp_cmd
        exit 4
    else
        rm $tmp_cmd
    fi
done
