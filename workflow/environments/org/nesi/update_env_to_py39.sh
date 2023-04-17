#! /usr/bin/sh

env_name=${1:?env_name argument missing}

export envs=/nesi/project/nesi00213/Environments
export myenv=$envs/$env_name

if [[ `hostname` =~ 'mahuika' ]] || [[ $HOSTNAME =~ 'wb' ]];then
    module load Python/3.9.9-gimkl-2020a

    python $myenv/workflow/environments/org/nesi/edit_cfg.py $myenv/virt_envs/python3_mahuika/pyvenv.cfg include-system-site-packages true

    ln -sf /opt/nesi/CS400_centos7_bdw/Python/3.9.9-gimkl-2020a/bin/python3.9 $myenv/virt_envs/python3_mahuika/bin/python3.9
    ln -sf $myenv/virt_envs/python3_mahuika/bin/python3.9 $myenv/virt_envs/python3_mahuika/bin/python3
    ln -sf $myenv/virt_envs/python3_mahuika/bin/python3 $myenv/virt_envs/python3_mahuika/bin/python

    # Add python_ver to the start of the bashrc so that python 3.9 will be loaded on next login
    sed -i '1iexport PYTHON_VER=3.9' ~/.bashrc

    python3 -m venv --upgrade $myenv/virt_envs/python3_mahuika

    source $myenv/virt_envs/python3_mahuika/bin/activate

elif [[ `hostname` =~ 'maui' ]] || [[ $HOSTNAME =~ 'ni' ]];then
    module load cray-python

    python $myenv/workflow/environments/org/nesi/edit_cfg.py $myenv/virt_envs/python3_maui/pyvenv.cfg include-system-site-packages true

    ln -sf /opt/python/3.9.13.2/bin/python3.9 $myenv/virt_envs/python3_maui/bin/python3.9
    ln -sf $myenv/virt_envs/python3_maui/bin/python3.9 $myenv/virt_envs/python3_maui/bin/python3
    ln -sf $myenv/virt_envs/python3_maui/bin/python3 $myenv/virt_envs/python3_maui/bin/python

    # Create the user specific mpi4py library
    MPICC="cc --shared" pip install --no-binary=mpi4py --force-reinstall --user mpi4py

    python3 -m venv --upgrade $myenv/virt_envs/python3_maui

    source $myenv/virt_envs/python3_maui/bin/activate

else
    echo "Could not detect machine, consult the software team for assistance."
    exit 1
fi

pip install -r $myenv/IM_calculation/requirements.txt -r $myenv/Empirical_Engine/requirements.txt -r $myenv/Pre-processing/requirements.txt -r $myenv/qcore/requirements.txt -r $myenv/visualization/requirements.txt -r $myenv/workflow/requirements.txt

pip install -I -e $myenv/Empirical_Engine/ -e $myenv/IM_calculation/ -e $myenv/Pre-processing/ -e $myenv/qcore/ -e $myenv/visualization/ -e $myenv/workflow/