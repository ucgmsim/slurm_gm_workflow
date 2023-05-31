
### Functions to load the python virtual environments

load_python3_maui () {
    load_python39_maui
}

load_python39_maui () {
    # Load python 3.9 and hdf5 library
    # No environment, as everyone should be using their own, we don't need a background one
    module load cray-python cray-hdf5-parallel/1.12.2.3
}

load_python3_mahuika () {
    # Not recommended
    # Keep this around as mahuika environments still work
    # Load python3, have to do this as virtualenv points to this python
    # verions, which is not accessible without loading
    module load Python/3.6.3-gimkl-2017a

    # Load the virtual environment
    source /nesi/project/nesi00213/share/virt_envs/python3_mahuika/bin/activate
}


load_python38_mahuika () {
    # Not recommended
    # Load python3, have to do this as virtualenv points to this python
    # verions, which is not accessible without loading
    module load Python/3.8.2-gimkl-2020a

    export gmsim='/nesi/project/nesi00213/Environments/python38' #override the default gmsim

    # Load the virtual environment
    source /nesi/project/nesi00213/Environments/python38/virt_envs/python3_mahuika/bin/activate
}


load_python39_mahuika () {
    # Python 3.9 is the new Maui version, so this keeps them in parallel
    # No background environment. If you want to do something special, make an environment
    module load Python/3.9.9-gimkl-2020a
}