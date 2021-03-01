Slurm Ground Motion Workflow
# Changelog
(Based on https://wiki.canterbury.ac.nz/download/attachments/58458136/CodeVersioning_v18p2.pdf?version=1&modificationDate=1519269238437&api=v2 )

## [19.6.27] - 2021-01-28 -- Added VM perturbations on install
### Added
    - Flags --vm_perturbations and --ignore_vm_perturbations for use with velocity model perturbations
    - If used the sim_params and root_params files will be updated for emod3d to use the relevant perturbation file

## [19.6.26] -  2020-12-04 -- Migrated to Jenkins CI
### Changed
    - Removed .travis.yml
    - Removed Travis-CI Webhooks from the repository setting
    - Removed the repository link from Travis-CI web. 
    - Jenkinsfile now controls the workflow of auto-testing
    - Added "jenkins" user and group to Docker image, so that the Docker image can be run not as root - preventing lots of files with root permission that need sudo to be cleaned up after testing
    - Can be manually kicked off testing.
    - Replaced build status badge

## [19.6.25] -  2020-03-16 -- Enabled automated Empirical calculation
### Added
    - Added Empirical calculation as a part of automated workflow
    - Run_cybershake can take a list as part of each task entry
    - Extra query modes for query_mgmt_db - todo and to list tasks that have exceeded max_retry count
    - Extra parameter for BB_Sim - ability to choose the site_amp model from config
### Changed
    - IM plot to use python 3 workflow
    - removed partition specifier for mahuika slurm scripts (as per NeSI recommendations)
    - lowered core requirements on quick animation, less queue time
    - Does not create new tasks for faults that are re-installed
    - When marking a task as failed it will mark that tasks depedencies as failed too. The output files are not touched
    - Removed default partition on slurm scripts for Mahuika as they aren't necessary anymore
    - Querymgmtdb now accepts lists as part of the task_config yaml entries.
### Updated
    - E2E tests bugs fixed

## [19.6.24] -  2020-03-10 -- Changed default HF to 6.0.3
### Changed
    - Changed the default version of HF to 6.0.3
    - Can be switched back to 5.4.5 with "--version 5.4.5" option for hf_sim.py
    - If version 6.0.3 is used, it utilizes a new argument--stress_param_adj

## [19.6.23] -  2020-02-11 -- Changed HF Rayset default to 1 
### Changed
    - Changed HF Rayset default from 1 2 to 1, which is internally handled as [1]. 

## [19.6.22] -  2019-12-19 -- Added Advanced_IM into automated workflow
### Added
    - Added slurm templates related to Advanced_IM
    - Added adv_im related logic to submit_sim_imcalc
    - Added simple tests for adv_im output to check for blank fields
### Changed
    - Updated log_metadata to handle adv_im related metadata
    - Updated automated workflow to be able to handle adv_im as a step
    - Updated queue_monitor to be able to handle a specific case where job is missing in queue and also not store in sacct history
    - Updated unit test to test for 15 steps intead of 14

## [19.6.21] - 2019-12-18 -- Removed stations along South and East edge of VM domain
### Removed
  - Stations along the South or Eastern edge of the VM domain had a broken component, so for now these stations have been removed from the station list.
### Changed
  - End to end test data has been updated to remove the dropped stations 


## [19.6.20] - 2019-12-13 -- Removed dashboard
### Removed
  - Dashboard has been relocated to a separete dashboard repo

## [19.6.19] - 2019-12-10 -- Added empirical calculation and aggregation orchestration scripts
### Added
  - Empirical calculation and aggregation scripts to the scripts/empirical directory
  - Empirical calculation and aggregation orchestration script to the scripts/empirical directory

## [19.6.18] - 2019-12-09 -- Updated Rrups and bug fixes
### Changed
  - Rrups has had some buggy behaviour removed, was previously failing due to bad bash
  - If an event or fault only has 1 realisation to be run it is installed without the \_REL suffix
  - Fixed auto close time out behaviour for run_cybershake and auto_submit, should finish as expected like previous behaviour 
  - Increased timeout to two iterations of each task subthread
  - Empiricals don't work yet, but have had updates to be closer to working

## [19.6.17] - 2019-11-29 -- Updated e2e test data
### Changed
  - Test data for v16p1 simple and validation, and 18p5p3p4 simple updated to have data for the updated workflow with timeshifting. Old test data is still available for testing old workflow versions.

## [19.6.16] - 2019-11-20 -- Add additional gmsim versions
### Added
  - Version files for gm versions 18.5.3.2, 18.5.3.1, 18.5.4.1

## [19.6.15] - 2019-11-01 -- Add timeshifting for LF
### Added
  - LF now has additional sim duration time added to account for time shifts to preserve causality
  - Version 3.0.4 of EMOD3D had a time shift of 1/flo, while proceeding versions have a time shift of 3/flo 

## [19.6.14] - 2019-09-23 --Update Dashboard app code
### Changed
  - Updated deprecated DashTable attributes so the app can run without error

## [19.6.13] - 2019-09-19
### Changed
  - For BB, HF2BB, LF2BB the dt can now be set and the given data will be interpolated to that level

## [19.6.12] - 2019-08-26 -- Add metadata for failed runs
### Added
  - Added metadata logging for failed runs in queue monitor

## [19.6.11] - 2019-08-21 -- Squeue failure detection
### Changed
  - If squeue does not return the expected headers, it is assumed to have failed. In this case no jobs will be marked as failed and requiring resubmitting
  - add_to_mgmt_queue now optionally takes in the slurm job id. If it is given it is used to match the update with an existing database entry, if it is not given the user is warned and the update is applied anyway
  - If more than one entry in the database is updated by an update the user is alerted to this

## [19.6.10] - 2019-08-16 -- Improved load balancing for HF calculation
### Changed
  - Updated the way station list is split to achieve improved load balancing for HF calculation
### Removed
  - Removed --seed: -1 option which allowed fully random run of HF calculation 
 
## [19.6.9] - 2019-08-14 --Added MIT license
### Added
  - Added MIT License

## [19.6.8] - 2019-08-12 -- Fixed IM_calculation completion check
### Changed
  - fixed logic bug checking completion of IM_calculation

## [19.6.7] - 2019-08-07 -- Add HF log aggregation
### Added
  - Added a script to aggregate HF logs into a csv with the amount of core hours lost to thread idling

## [19.6.6] - 2019-07-26 -- Four hpc logins for dashboard 
### Changed
  - Dashboard now try 4 hpc login nodes [maui|mahuika][01|02] instead of 2

## [19.6.5] - 2019-07-26 -- Moved logging to qcore
### Removed
  - Logging has been moved to the qcore repository to allow its use in other repositories  

## [19.6.4] - 2019-07-19 -- Added verification plots
### Added
  - Added IM_plot, plot_ts and plot_srf slurm scripts to cybershake workflow
### Fixes
  - Updated rrup to run as expected
    
## [19.6.3] - 2019-07-11 -- EMOD3D dump and nt check
### Changed
  - The nt check on install now rounds to the nearest integer, instead of rounding down to the next one.

### Removed
  - EMOD3D no longer dumps partial results to the output directory.

## [19.6.2] - 19-07-11 -- Changelog catchup
### Added
  - Two more testing configurations to end to end tests
  - Queue monitor stress test
  - IM_calculation and visualisation added to environment creation
  - Checks to install and BB_sim to ensure lf and hf have the same number of (extrapolated) steps
  - On start any tasks that can have retires with the current max_n_retries have them added
  - The flag -c is now available for the query_mgmt_db script to give a count of how many tasks are in each state
  - plot_ts and plot_srf added
  - Added loading test to test_merge_ts
  - Template task_config for the automated wrapper
    
### Changed
  - Improved cross platform support for slurm scripts from Maui to Mahuika
  - Queue monitor is now responsible for checking squeue and keeping the database up to date
  - Improved logging for threaded scripts
  - Shared library refactored out of shared: shared_automated_workflow
  - slurm log file names are now determined by job number and name
  - rrup renabled

## [19.6.1] - 2019-06-05 -- Error task changes
### Added
  - A wrapper for auto_submit and queue_monitor is now available 
  - LF and HF may now be converted to BB without the other, and IM calculations subsequently performed on them
### Changed
  - When tasks fail they will have a new task made for them instead of having their retry counter incremented

## [19.4.11] - 2019-05-14 -- ASCII workflow removed
### Removed
  - The parts of the workflow relating to text based computation outputs have been removed
    - This includes winbin_aio, match_seismo and hfsims-stats
### Changed
  - version is now the final parameter and is optional. If it is not provided it is assumed to be version 16.1
  - Cybershake path is now converted to the absolute path of what is passed in

## [19.4.10] - 2019-05-06 -- Srf validated against VM bounds
### Added
  - The first srf of each fault is checked that it is within the bounds of the velocity model. 
  - As a result, out of bounds srfs will now cause validation to fail.

## [19.4.9] - 2019-05-03 -- Cybershake file removed
### Changed
  - HF seed is now an optional parameter to the install script
  - Instead of passing the path to a cybershake config file, the required version should be passed to the install script
### Removed
  - Cybershake_config.json no longer needed. All relevant values have been moved to the root_defaults.yaml in the gmsim templates subdirectory.

## [19.4.8] - 2019-05-01 -- End to End test improvements
### Changed
  - Improved submit and mgmt queue logging
  - Changed to work with new automated workflow submit (see [19.4.2])
  - Lots of minor improvements so it can handle running/testing a large quantity of simulations.
    
## [19.4.7] - 2019-04-30 -- Estimation unit tests
### Changed
  - Added core hours estimation unit tests 

## [19.4.6] - 2019-04-18 -- LF zero test
### Changed
  - Added extra LF test that checks if there are any zeros in the velocities 

## [19.4.5] - 2019-04-17 -- HF seed propagated
### Changed
  - Installation paths can now be provided as relative paths 
  - Files in the management db folder are not deleted if they are not valid update files
  - HF seeds are now correctly communicated to the child processes
  - LF checkpoint files are now removed when clean up occurs
  - merge_ts now accepts absolute paths 

## [19.4.4] - 2019-04-16 -- Install path update
### Changed
  - The relative path to the cybershake directory can now be passed to install_cybershake
  - Changelog now has newest first


## [19.4.3] - 2019-04-12 -- Dashboard
### Changed
    Changes to dashboard:
  - Changed to use sreport to get total & daily core hours usage
  - Changed dashboard test functions & test zip file download path
  - Tidied up Maui dashboard
  - Added Mahuika total core hours usage to dashboard
  - Added functionalities to collection of old total core hours usgae
  - fixed logic bug in inserting into maui/mahuika daily table


## [19.4.2] - 2019-04-10 -- QSW_1057 - Sqlite locking fix
### Changed
    Changes to automated workflow:
  - Split into two processes:
      - Auto-submit: which submits job to the HPC & populates the mgmt db queue with
        updates for the mgmt db
      - Queue monitor: Updates the status of the tasks in mgmt db
    
    For how to use, see the updated [README](https://github.com/ucgmsim/slurm_gm_workflow/blob/master/README.md)
    
    Note: The auto-submit reads from the mgmt db, but NEVER writes or updates

## [19.4.1] - 2019-04-09 -- Initial Version
### Changed
    Changes to autosubmit:
  - Machine queues are maintained individually
  - The desired maximum length for all or each machine queue can be set with the -n flag
