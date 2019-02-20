
### Functions to load the python virtual environments

load_python2_maui () {
    # Load python2
    module load cray-python/2.7.15.1

    # Removed python3
    module del cray-python/3.6.5.1

        # Reset the PYTHONPATH
        export PYTHONPATH=/opt/python/2.7.15.1/lib/python2.7/site-packages

        # PYTHONPATH (this can be removed once qcore is installed as a pip package)
        export PYTHONPATH=/nesi/project/nesi00213/opt/maui/qcore:$PYTHONPATH

        # PYTHONPATH for workflow
        export PYTHONPATH=/nesi/project/nesi00213/workflow:$PYTHONPATH

        # Load the virtual environment
        source /nesi/project/nesi00213/share/virt_envs/python2_maui/bin/activate
}

load_python3_maui () {
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

        # Load the virtual environment
        source /nesi/project/nesi00213/share/virt_envs/python3_maui/bin/activate
}

load_python2_mahuika () {
    # Load python2, have to do this as virtualenv points to this python
    # verions, which is not accessible without loading
    module load Python/2.7.14-gimkl-2017a

        # Reset the PYTHONPATH
        export PYTHONPATH=''

        # PYTHONPATH (this can be removed once qcore is installed as a pip package)
        export PYTHONPATH=/nesi/project/nesi00213/opt/mahuika/qcore:$PYTHONPATH

        # PYTHONPATH for workflow
        export PYTHONPATH=/nesi/project/nesi00213/workflow:$PYTHONPATH

        # Load the virtual environment
        source /nesi/project/nesi00213/share/virt_envs/python2_mahuika/bin/activate
}

load_python3_mahuika () {
    # Load python3, have to do this as virtualenv points to this python
    # verions, which is not accessible without loading
    module load Python/3.6.3-gimkl-2017a

        # Reset the PYTHONPATH
        export PYTHONPATH=''

        # PYTHONPATH (this can be removed once qcore is installed as a pip package)
        export PYTHONPATH=/nesi/project/nesi00213/opt/mahuika/qcore:$PYTHONPATH

        # PYTHONPATH for workflow
        export PYTHONPATH=/nesi/project/nesi00213/workflow:$PYTHONPATH

        # Load the virtual environment
        source /nesi/project/nesi00213/share/virt_envs/python3_mahuika/bin/activate
}

deactivate_virtenv () {
    # Reset the pythonpath
    export PYTHONPATH=''

    deactivate
    source ~/.bashrc
}


