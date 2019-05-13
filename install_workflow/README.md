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
1) Go to an existing workflow repository
2) Navigate to ".../slurm_gm_workflow/install_workflow" 
3) Run 
    ```bash
    ./create_env.sh environment_name config_to_use
    ```
    where a default config (env_config.json) is located in the same directory, which should
    work without requiring any changes.  
    Note: The environment is installed into /nesi/project/nesi00213/Environments/
    so if an environment with the same already exists the script will exit.

4) Check that the script ran to completion without any errors, 
apart from the IM_calculation setup warning and the pip qcore error.
5) Log into mahuika
6) Navigate to the new environment, and into the slurm_gm_workflow/install_workflow
7) Run
    ```bash
    ./create_python_virtenv_mahuika.sh env_path
    ```
    where env_path is /nesi/projects/nesi00213/Environments/env_name

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


