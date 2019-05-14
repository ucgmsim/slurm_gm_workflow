Slurm Ground Motion Workflow
# Changelog
(Based on https://wiki.canterbury.ac.nz/download/attachments/58458136/CodeVersioning_v18p2.pdf?version=1&modificationDate=1519269238437&api=v2 )

## [19.4.11] - 2019-05-14 -- Changes to install_cybershake
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
