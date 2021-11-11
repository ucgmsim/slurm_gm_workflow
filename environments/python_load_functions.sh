
### Functions to load the python virtual environments

load_python3_maui () {
    # Removed default python2 module
    module del cray-python
    # Load python3
    module load cray-python/3.6.5.1

}

load_python3_mahuika () {
    # Load python3, have to do this as virtualenv points to this python
    # verions, which is not accessible without loading
    module load Python/3.6.3-gimkl-2017a

}

