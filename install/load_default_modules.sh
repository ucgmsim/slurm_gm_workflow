#scripts to load some basic modules on different machines
module load slurm

if [[ `hostname` = mahuika02 ]];then
    module load Python/2.7.14-gimkl-2017a
    module load PrgEnv-cray/1.0.4
    echo 'loaded mahuika module'; 
elif [[ `hostname` = maui01 ]];then 
    module load cray-python/2.7.15.1
    echo 'loaded maui module'; 
else
    #Kupe use case
    module load mpi4py
fi

