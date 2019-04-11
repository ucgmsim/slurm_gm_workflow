Slurm Ground Motion Workflow
# Changelog
(Based on https://wiki.canterbury.ac.nz/download/attachments/58458136/CodeVersioning_v18p2.pdf?version=1&modificationDate=1519269238437&api=v2 )

## [19.4.1] - 2019-04-09 -- Initial Version
### Changed
Changes to autosubmit: 
- Machine queues are maintained individually
- The desired maximum length for all or each machine queue can be set with the -n flag
    
## [19.4.2] - 2019-04-10 -- QSW_1057 - Sqlite locking fix
### Changed
Changes to automated workflow:
- Split into two processes:
    - Auto-submit: which submits job to the HPC & populates the mgmt db queue with
    updates for the mgmt db
    - Queue monitor: Updates the status of the tasks in mgmt db  
        
For how to use, see the updated [README](https://github.com/ucgmsim/slurm_gm_workflow/blob/master/README.md)
        
Note: The auto-submit reads from the mgmt db, but NEVER writes or updates 
    
      