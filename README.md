[![Build Status](https://travis-ci.org/ucgmsim/slurm_gm_workflow.svg?branch=master)](https://travis-ci.org/ucgmsim/slurm_gm_workflow)

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
/path/to/cybershake_config.json /path/to/list.txt 
```
where the cybershake config looks something like this

```json
{
    "global_root" : "/nesi/project/nesi00213" ,
    "stat_file_path" : "/nesi/project/nesi00213/StationInfo/non_uniform_whole_nz_with_real_stations-hh400_v18p6.ll",
    "v_1d_mod"  :   "/nesi/project/nesi00213/VelocityModel/Mod-1D/Cant1D_v3-midQ_OneRay.1d",
    "dt"    : 0.02,
    "hf_dt" : 0.005,
    "rand_reset" : true,
    "hf_seed": 0,
    "extended_period": false,
    "version": "16.1"
}
```

and list.txt is a list of the faults to run, along with the number of realisations, e.g.
```
Hossack 4r
RepongaereF4 10r
```

### Running
To run the installed simulations, navigate to the rootDir and run
```bash
python $gmsim/workflow/scripts/cybershake/auto_submit.py /path/to/rootDir username 
--config /path/to/cybershake_config.json 
```
this will start submitting the different task on the HPC.
In order to keep the management DB up to date the following has to be run in a seperate 
process:
```bash
$gimsim/workflow/scripts/cybershake/queue_monitor /path/to/rootDir
```


