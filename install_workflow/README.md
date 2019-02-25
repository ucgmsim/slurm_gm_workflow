## Environment installation

Requirements: Setup github SSH keys for maui, 
[guide](https://help.github.com/en/articles/connecting-to-github-with-ssh) for how to do this. 

A user specific environment of workflow, qcore, and IMCalc can be created as follows:  
1) Go to an existing workflow repository or clone a new one
2) Navigate to ".../slurm_gm_workflow/install_workflow" 
3) Run 
    ```bash
    ./create_env.sh environment_name config_to_use
    ```
    where a default config is located in the same directory, which should
    work without requiring any changes.  
    Note: The environment is installed into /nesi/project/nesi00213/Environments/
    so if an environment with the same already exists the script will exit.

4) Check that the script ran to completion without any errors, 
apart from the IM_calculation setup warning, which can be ignored.

The new environment can then be activated with 
```bash
activate_env /nesi/project/nesi00213/Environments/environment_name
```

and deactivated with 
```bash
deactivate_env
```

Note: Activating an environment will update your $PYTHONPATH and $gmsim variables
These will be reset to the default shared bashrc when deactivating the environment.


##### Modifying an environment
Update any of your repositories as per usual with git



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


