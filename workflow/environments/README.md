## Environment installation

### Usage 
Environment can be activated with 
```bash
activate_env /nesi/project/nesi00213/Environments/environment_name
```

and deactivated with 
```bash
deactivate_env
```

### Installation

Requirements: Setup github SSH keys for maui, 
[guide](https://help.github.com/en/articles/connecting-to-github-with-ssh) for how to do this. 

**All paths need to be absolute**

A user specific environment of workflow, qcore, IMCalc, Empirical Engine and Pre-processing 
can be created as follows:  
1) Log in to Maui, and go to an existing copy of workflow (eg. can be your home) - it is a good idea to update this copy. If you don't have it, 
```bash
git clone git@github.com:ucgmsim/slurm_gm_workflow.git
```
2) Navigate to the copy of your workflow "slurm_gm_workflow/workflow/environments/org/nesi"
3) Load the correct version of Python module. (**IMPORTANT**). For example, Python 3.9, try the following.
   ```bash
   module load cray-python cray-hdf5-parallel/1.12.2.3
   ```
4) Run the following command.
    ```bash
    ./create_env_maui.sh ENV_NAME ./env_config.json
    ```
    where a default config (env_config.json) is located in the same directory, which should
    work without requiring any changes. 
    Note: The environment is created at /nesi/project/nesi00213/Environments/ENV_NAME
    so if an environment with the same already exists the script will exit.

5) Check if the script ran to completion. You may see a few error messages. Don't panic.
- You may first encounter some errors and issues with numpy, scipy etc., as it can't uninstall the existing pre-installed modules from the system python, which will mostly reconcile automatically as it progresses.
- You will most likely to have an issue with mpi4py, complaining about `mpi.h`. It is ok, as mpi4py is already pre-installed in the system python 3.9 (Note: python 3.8 does not have mpi4py installed in the system environment and needs to be manually installed) 
- You may have an issue with the IM_calculation setup warning and the pip qcore error. You can try to reinstall afterwards. If you need to retry pip install, make sure you activate the new environment first. For example, if your environment is located at `/nesi/project/nesi00213/Environments/sjn872033`, and you wish to reinstall `XXX`,
```bash
source /nesi/project/nesi00213/Environments/sjn872033/virt_envs/python3_maui/bin/activate 
pip install XXX --upgrade
```
- Failing to activating the environemt and running `pip install` will end up with the packages installed under `$HOME/.local/lib`. Double-check if this directory has anything installed. Anything installed there can get in the way, and loaded instead of the one installed in the environment.

6) Log into mahuika
7) Add this line in your .bashrc
```
export PYTHON_VER=3.9
```
above the line of
```
source /nesi/project/nesi00213/share/bashrc.uceq
```
Log out and log back in to Mahuika.
This will make sure you load the correct version of Python module. ie, the following command is automatically executed when you log in.
```bash
module purge --force NeSI;module add NeSI Python/3.9.9-gimkl-2020a
```

8) When you are logged in, make sure the version you want is correctly loaded.
```bash
which python
/opt/nesi/CS400_centos7_bdw/Python/3.9.9-gimkl-2020a/bin/python
```

9) Navigate to the `slurm_gm_workflow/workflow/environments/org/nesi`. This can be the one you used to make Maui environment in your home directory.
10) Run
    ```bash
    ./create_python_virtenv_mahuika.sh env_path
    ```
    where env_path is the full path of /nesi/project/nesi00213/Environments/ENV_NAME

Notes: 
- Activating an environment will update your $PYTHONPATH and $gmsim variables
These will be reset to the default shared bashrc when deactivating the environment.
- Activating an environment will also set a CUR_ENV and CUR_HPC environment variable,
these are required for cross platform submission when using an environment.

#### Updating local packages
Packages that are cloned into the environment (such as qcore) can be updated using
```
pip install -I --no-deps ./qcore
```

### Config
The config allows changing of the base path for the environments.
```json
{
  "base_environments_path": "/nesi/project/nesi00213/Environments"
}
```


-----------------------------------------------------------

## Old Installation

This directory contains all the necessary scripts to prepare
a functional installation of the Slurm based gm_sim_workflow

### Usage

One needs to run the script like:
```bash
./deploy_workflow.sh $TARGET_DIRECTORY
```

This will create all the necessary directories and copy a bunch of files to `$TARGET_DIRECTORY`. 

Note: you need to have the qcore and EMOD3D codes on `$TARGET_DIRECTORY` for the full workflow to function.


