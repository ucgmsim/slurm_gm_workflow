#scripts to load some basic modules on different machines
module load slurm

if [[ `hostname` =~ 'mahuika' ]] || [[ $HOSTNAME =~ 'wb' ]];then
    # Load python3, have to do this as virtualenv points to this python
    # verions, which is not accessible without loading
    module load Python/3.6.3-gimkl-2017a

    # Reset the PYTHONPATH
    export PYTHONPATH=''

    # PYTHONPATH (this can be removed once qcore is installed as a pip package)
    export PYTHONPATH=/nesi/project/nesi00213/opt/mahuika/qcore:$PYTHONPATH

    # PYTHONPATH for workflow
    export PYTHONPATH=/nesi/project/nesi00213/workflow:$PYTHONPATH

    module load PrgEnv-cray/1.0.4
    module load GDAL/2.2.2-gimkl-2017a-GEOS-3.5.1
    module del LibTIFF/4.0.7-gimkl-2017a
    #python libs
    export PATH=/nesi/project/nesi00213/opt/mahuika/python-packages/bin:$PATH
    #gmt
    export PATH=/nesi/project/nesi00213/opt/mahuika/gmt/5.4.4/bin:$PATH
    export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/nesi/project/nesi00213/opt/mahuika/gmt/5.4.4/lib64
elif [[ `hostname` =~ 'maui' ]] || [[ $HOSTNAME =~ 'ni' ]];then 
    # Load python3
    module load cray-python/3.6.5.1

    # Removed python2
    module del cray-python/2.7.15.1

    # Reset the PYTHONPATH
    export PYTHONPATH=/opt/python/3.6.5.1/lib/python3.6/site-packages

    # PYTHONPATH (this can be removed once qcore is installed as a pip package)
    export PYTHONPATH=$PYTHONPATH:/nesi/project/nesi00213/opt/maui/qcore

    # PYTHONPATH for workflow
    export PYTHONPATH=$PYTHONPATH:/nesi/project/nesi00213/workflow
    #python libs
    export PATH=/nesi/project/nesi00213/opt/maui/python-packages/bin:$PATH
else
    #Failed to indentify hostname, print out for debug
    echo "cannot identiy hostname."
    echo "hostname: $HOSTNAME"
fi

