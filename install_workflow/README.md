## Installation

This directory contains all the necessary scripts to prepare
a functional installation of the Slurm based gm_sim_workflow

### Usage

One needs to run the script like:
```bash
./deploy_workflow.sh $TARGET_DIRECTORY
```

This will create all the necessary directories and copy a bunch of files to `$TARGET_DIRECTORY`. 

Note: you need to have the qcore and EMOD3D codes on `$TARGET_DIRECTORY` for the full workflow to function.