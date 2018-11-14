#scripts to load some basic modules on different machines
module load slurm

if [[ `hostname` = mahuika02 ]] || [[ $HOSTNAME =~ 'wb' ]];then
    module load Python/2.7.14-gimkl-2017a
    module load PrgEnv-cray/1.0.4
    module load GDAL/2.2.2-gimkl-2017a-GEOS-3.5.1
    module del LibTIFF/4.0.7-gimkl-2017a
    #python libs
    export PYTHONPATH=/nesi/project/nesi00213/opt/mahuika/python-packages/lib/python2.7/site-packages:$PYTHONPATH
    export PATH=/nesi/project/nesi00213/opt/mahuika/python-packages/bin:$PATH
    #qcore
    export PYTHONPATH=/nesi/project/nesi00213/opt/mahuika/qcore:$PYTHONPATH
    #gmt
    export PATH=/nesi/project/nesi00213/opt/mahuika/gmt/5.4.4/bin:$PATH
    export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/nesi/project/nesi00213/opt/mahuika/gmt/5.4.4/lib64
elif [[ `hostname` = maui01 ]] || [[ $HOSTNAME =~ 'ni' ]];then 
    module load cray-python/2.7.15.1
    #python libs
    export PYTHONPATH=/nesi/project/nesi00213/opt/maui/python-packages/lib/python2.7/site-packages:$PYTHONPATH
    export PATH=/nesi/project/nesi00213/opt/maui/python-packages/bin:$PATH
    #qcore
    export PYTHONPATH=/nesi/project/nesi00213/opt/maui/qcore:$PYTHONPATH
else
    #Failed to indentify hostname, print out for debug
    echo "cannot identiy hostname."
    echo "hostname: $HOSTNAME"
fi

