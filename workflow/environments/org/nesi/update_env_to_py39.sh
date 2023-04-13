#! /usr/bin/sh

env_name=${1:?env_name argument missing}

export envs=/nesi/project/nesi00213/Environments
export myenv=$envs/env_name

if [[ `hostname` =~ 'mahuika' ]] || [[ $HOSTNAME =~ 'wb' ]];then

    hpc='mahuika'
    ln -sf /opt/nesi/CS400_centos7_bdw/Python/3.9.9-gimkl-2020a/bin/python $myenv/virt_envs/python3_mahuika/bin/python3
    source $myenv/virt_envs/python3_maui/bin/activate

elif [[ `hostname` =~ 'maui' ]] || [[ $HOSTNAME =~ 'ni' ]];then
    hpc='maui'

    module load cray-python

    ln -sf /opt/python/3.9.13.2/bin/python $myenv/virt_envs/python3_maui/bin/python
    # Build the mpi4py library into the users home site-packages
    MPICC="cc --shared" pip install --no-binary=mpi4py --force-reinstall --user mpi4py
    source $myenv/virt_envs/python3_maui/bin/activate

else
    echo "Could not detect machine, consult the software team for assistance."
    exit 1
fi

pip install -r $myenv/IM_calculation/requirements.txt -r $myenv/Empirical_Engine/requirements.txt -r $myenv/Pre-processing/requirements.txt -r $myenv/qcore/requirements.txt -r $myenv/visualization/requirements.txt -r $myenv/workflow/requirements.txt

pip install -I -e $myenv/Empirical_Engine/ -e $myenv/IM_calculation/ -e $myenv/Pre-processing/ -e $myenv/qcore/ -e $myenv/visualization/ -e $myenv/workflow/