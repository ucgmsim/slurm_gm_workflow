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
if [[ ! -z ${SLURM_CPUS_ON_NODE} ]] && [[ ${SLURM_NTASKS} != ${fileCount} ]]
then
    echo "Number of cores and number of log files mismatch"
    exit 1
fi

rlog_count=0
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

    #Check that if the Rlog says it wrote the xyts file, then that file exists
    grep "xy-plane time slice:" $rlog >> /dev/null
    if [[ $? == 0 ]] && [[ ! -f "$xytsFile" ]];
    then
        echo "The Rlog said it was going to write the xyts file, but we could not find it: $xytsFile"
        rlog_check=1
        break
    fi

    #Check that if the Rlog says it wrote the seis file, then that file exists
    grep "ALL seismograms written into single output file" $rlog >> /dev/null
    if [[ $? == 0 ]] && [[ ! -f "$seisFile" ]];
    then
        echo "The Rlog said it was going to write the seis file, but we could not find it: $seisFile"
        rlog_check=1
        break
    fi
done

# Check the integrity of the seisfiles only if all files that are expected to be there are present
# This is done by attempting to load them into LFSeis which will check them
if [[ $rlog_check == 0 ]];
then
    python $gmsim/workflow/workflow/calculation/verification/test_lf_seis.py ../OutBin
    seisIntegrity=$?
fi

# EMOD3D is considered to be completed if:
#  - Every Rlog contains the string "IS FINISHED"
#  - In every Rlog file, if it says it wrote a seis or xyts file, then that file is present
#  - Every seis file is loaded by LFSeis without an issue
if [[ $rlog_check == 0 ]] && [[ $seisIntegrity == 0 ]];
then
    echo "$lf_sim_dir: EMOD3D completed"
    exit 0
else
    echo "$lf_sim_dir: EMOD3D not completed"
    exit 1
fi


