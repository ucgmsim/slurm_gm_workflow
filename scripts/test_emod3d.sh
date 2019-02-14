#!/bin/bash

#get run_name from $1

if [[ $# -lt 2 ]]; then
    echo "please provide the path to sim_dir and srf_name"
    exit 1
fi

#get model list

sim_dir=$1
srf_name=$2
lf_sim_dir=$sim_dir/LF

#check Rlog
cd $lf_sim_dir/Rlog 2> /dev/null
#abort rest of the code if cd returned not 0 (folder does not exist, emod3d has not yet been run)
if [[ $? != 0 ]]; then
    echo "$lf_sim_dir: have not yet run EMOD3D"
    exit 1
fi

fileCount=`ls -1|wc -l`

#Check that the number of cores and number of rlog files is the same
if [[ ! -z ${SLURM_CPUS_ON_NODE} ]] && [[ ${SLURM_CPUS_ON_NODE} != ${fileCount} ]]
then
    echo "Number of cores and number of log files mismatch"
    exit 1
fi

rlog_count=$(expr 0)
rlog_check=0
for rlog in *;
do
    rlog_count=`expr $rlog_count + 1`
    grep "IS FINISHED" $rlog >>/dev/null
    if [[ $? == 0 ]];
    then
        rlog_check=0
#            echo "rlog =1"
    else
        rlog_check=1
        echo "$rlog not finished"
        break
    fi

    rootFile="../OutBin/${rlog/%.rlog/.e3d}"

    seisFile=`echo $rootFile | sed 's/\(.*\)-/\1_seis-/'`
    xytsFile=`echo $rootFile | sed 's/\(.*\)-/\1_xyts-/'`

    #Check that if the seis or xyts file exists for a certain integer then the other also exists
    if [[ -f "$seisFile" ]] && [[ ! -f "$xytsFile" ]];
    then
        echo "Found the seis file $seisFile, but didn't find the matching xyts file $xytsFile"
        rlog_check=1
        break
    fi
    if [[ ! -f "$seisFile" ]] && [[ -f "$xytsFile" ]];
    then
        echo "Found the xyts file $xytsFile, but didn't find the matching seis file $seisFile"
        rlog_check=1
        break
    fi
done

#Check the integrity of the seisfiles
if [[ $rlog_check == 0 ]];
then
    python3 -c "from qcore import timeseries; timeseries.LFSeis('../OutBin');" 2>/dev/null
    seisIntegrity=$?
    if [[ $seisIntegrity != 0 ]];
    then
        echo "At least one file failed the integrity check"
    fi
fi

if [[ $rlog_check == 0 ]] && [[ $seisIntegrity == 0 ]];
then
    echo "$lf_sim_dir: EMOD3D completed"
    exit 0
else
    echo "$lf_sim_dir: EMOD3D not completed"
    exit 1
fi


