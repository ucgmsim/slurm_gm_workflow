"""
@author: jam335 - jason.motha@canterbury.ac.nz

A script that updates a slurm mgmt db and updates the status of a task
"""

import argparse
import create_mgmt_db

def update_db(db, process, status, job=None, run_name=None, error=None):
    #update the status of a task where a task has progressed a state
    #If a task needs to go backwards a new entry should be created instead
    db.execute('''UPDATE state
                  SET status = (SELECT id FROM status_enum WHERE state = ?), 
                      last_modified = strftime('%s','now'),
                      job_id = ?,
                      error = ?
                  WHERE (job_id = ? or run_name = ?)
                   AND proc_type = (SELECT id FROM proc_type_enum WHERE proc_type = ?)
                   AND status <= (SELECT id FROM status_enum WHERE state = ?)''', (status, job, error, job, run_name, process, status))
    db.connection.commit()    

def force_update_db(db, process, status, job=None, run_name=None, error=None):
    db.execute('''UPDATE state
                  SET status = (SELECT id FROM status_enum WHERE state = ?), 
                      last_modified = strftime('%s','now'),
                      job_id = ?,
                      error = ?
                  WHERE (job_id = ? or run_name = ?)
                   AND proc_type = (SELECT id FROM proc_type_enum WHERE proc_type = ?)
                   ''', (status, job, error, job, run_name, process))    
    db.connection.commit()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('run_folder', type=str, 
                        help="folder to the collection of runs on Kupe")
    parser.add_argument('process', choices=['EMOD3D', 'merge_ts', 'winbin_aio', 'HF', 'BB', 'IM_calculation'])
    parser.add_argument('status', type=str, choices=['created','in-queue','running','completed','failed'])
    parser.add_argument('-r', '--run_name', type=str,
                        help='name of run to be updated')
    parser.add_argument('-j', '--job', type=int)
    parser.add_argument('-e' ,'--error', type=str)
   
    parser.add_argument('-f', '--force', action="store_true")
     
    args = parser.parse_args()
    f = args.run_folder
    process = args.process
    status = args.status
    run_name = args.run_name
    job_id = args.job
    error = args.error
    db = create_mgmt_db.connect_db(f)
    
    if args.force:
        force_update_db(db, process, status, job=job_id, run_name=run_name, error=error)
    else:
        update_db(db, process, status, job=job_id, run_name=run_name, error=error)

    db.connection.commit()

if __name__ == '__main__':
    main()
