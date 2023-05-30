#scripts to load some basic modules on different machines
module load slurm

if [[ `hostname` == mahuika* ]] || [[ $HOSTNAME == wb* ]] || [[ $HOSTNAME == vgpuwb* ]];then
    # Library required for gmt
    module load GDAL/3.0.4-gimkl-2020a
    #python libs
    export PATH=/nesi/project/nesi00213/opt/mahuika/python-packages/bin:$PATH
    #gmt
    export PATH=/nesi/project/nesi00213/opt/mahuika/gmt/5.4.4_gimkl_2020a/bin:$PATH
    #ffmpeg
    export PATH=/nesi/project/nesi00213/opt/mahuika/ffmpeg_build/bin:$PATH
    #NZVM
    export PATH=/nesi/project/nesi00213/opt/mahuika/Velocity-Model:$PATH
    
    export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/nesi/project/nesi00213/opt/mahuika/gmt/5.4.4_gimkl_2020a/lib64
    load_python3_mahuika

elif [[ `hostname` == maui* ]] || [[ $HOSTNAME == ni* ]];then
    #python libs
    export PATH=/nesi/project/nesi00213/opt/maui/python-packages/bin:$PATH
    #NZVM
    export PATH=/nesi/project/nesi00213/opt/maui/Velocity-Model:$PATH
    # custom build modules 
    export MODULEPATH=/nesi/project/nesi00213/opt/maui/modules/all:$MODULEPATH

    load_python3_maui
elif [[ `hostname` == 'w-maui*']] || [[ $HOSTNAME == ws* ]] || [[ $HOSTNAME == vgpuws* ]];then
    # On a Maui ancillary node
    :
else
    #Failed to indentify hostname, print out for debug
    echo "cannot identiy hostname."
    echo "hostname: $HOSTNAME"
fi

