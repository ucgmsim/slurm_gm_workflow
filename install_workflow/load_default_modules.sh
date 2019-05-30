#scripts to load some basic modules on different machines
module load slurm

if [[ `hostname` == mahuika* ]] || [[ $HOSTNAME == wb* ]] || [[ $HOSTNAME == vgpuwb* ]];then
    module load PrgEnv-cray/1.0.4
    module load GDAL/2.2.2-gimkl-2017a-GEOS-3.5.1
    module del LibTIFF/4.0.7-gimkl-2017a
    #python libs
    export PATH=/nesi/project/nesi00213/opt/mahuika/python-packages/bin:$PATH
    #gmt
    export PATH=/nesi/project/nesi00213/opt/mahuika/gmt/5.4.4/bin:$PATH
    #NZVM
    export PATH=/nesi/project/nesi00213/opt/mahuika/Velocity-Model:$PATH
    
    export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/nesi/project/nesi00213/opt/mahuika/gmt/5.4.4/lib64
    load_python3_mahuika

elif [[ `hostname` == maui* ]] || [[ $HOSTNAME == ni* ]];then
    #python libs
    export PATH=/nesi/project/nesi00213/opt/maui/python-packages/bin:$PATH
    #NZVM
    export PATH=/nesi/project/nesi00213/opt/maui/Velocity-Model:$PATH

    load_python3_maui
elif [[ `hostname` == 'w-maui*']] || [[ $HOSTNAME == ws* ]] || [[ $HOSTNAME == vgpuws* ]];then
    # On a Maui ancillary node
    :
else
    #Failed to indentify hostname, print out for debug
    echo "cannot identiy hostname."
    echo "hostname: $HOSTNAME"
fi

