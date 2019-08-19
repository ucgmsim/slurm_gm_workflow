[![Build Status](https://travis-ci.org/ucgmsim/slurm_gm_workflow.svg?branch=master)](https://travis-ci.org/ucgmsim/slurm_gm_workflow)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## Slurm GM Simulation Workflow

Contains the following:
- Scripts for installing and usage of HPC environments, which allow running of 
the workflow in a sandbox
- Scripts/Code for installing and running the automated workflow. This includes:
    - Scripts for installing the a automated workflow run
    - Execution of the installed simulations
    - Management DB code, along with scripts to keep the management DB up to 
    date during an automated workflow run
    - Slurm templates
    - Code for metadata collection
- Unittests
- Code/Scripts for estimation of core hour usage
- Code/Scripts for collection of HPC usage information and displaying it on a
dashboard.
- End-to-End tests of the automated workflow

### Running the automated workflow

Also see [cybershake manual](https://wiki.canterbury.ac.nz/display/QuakeCore/Cybershake+Run+Manual).

#### Setup
To run an automated workflow the following folder strucutre/files are required:
- rootDir/Data/ 
    - Sources  
        - Faults (folder for each)  
            - Srf
                - .srf files (one for each realisation)
                - .info files (one for each realisation)
            - Stoch
                - .stoch file (one for each realisation)
    - VMs
        - Faults (folder for each)
            - Lots of files (?) 

where rootDir is the directory to which the automated workflow will be installed.

To generate these files see [pre-processing](https://github.com/ucgmsim/Pre-processing) 
and [cybershake manual](https://wiki.canterbury.ac.nz/display/QuakeCore/Cybershake+Run+Manual).

#### Installing
Run the install_cybershake.sh bash script, e.g.
```bash
$gmsim/workflow/scripts/cybershake/install_cybershake path/to/rootDir 
cybershake_version /path/to/list.txt --seed [seed]
```
where the cybershake version is a string with a relevant subdirectory in the templates folder.
Current valid options are 16.1 and 18.5.3.4

and list.txt is a list of the faults to run, along with the number of realisations, e.g.
```
Hossack 4r
RepongaereF4 10r
```
seed is an optional integer argument to specify the seed to be used for HF calculations. If it is not given, a value in SEED file will be used if the file exists. Otherwise, a random seed will be chosen and kept in SEED file for next use.

### Running
To run the installed simulations, copy the task_config.yaml configuration file to the run directory and modify it to 
your needs. Run the cybershake with the following script:
```bash
python $gmsim/workflow/scripts/cybershake/run_cybershake.py /path/to/rootDir /path/to/rootDir/task_config.yaml <user name>
```
this will start submitting the different tasks on the HPC and will keep the database up to date.

