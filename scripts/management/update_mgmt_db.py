"""
@author: jam335 - jason.motha@canterbury.ac.nz

A script that updates a slurm mgmt db and updates the status of a task
"""

import argparse
import db_helper


def update_db(db, process, status, job=None, run_name=None, error=None):
    # update the status of a task where a task has progressed a state
    db.execute('''UPDATE state
                  SET status = (SELECT id FROM status_enum WHERE state = ?), 
                      last_modified = strftime('%s','now'),
                      job_id = ?
                  WHERE (job_id = ? or run_name = ?)
                   AND proc_type = (SELECT id FROM proc_type_enum WHERE proc_type = ?)
                   AND status <= (SELECT id FROM status_enum WHERE state = ?)''',
               (status, job, job, run_name, process, status))

    if run_name is not None and (status == db_helper.State.running.name or status == db_helper.State.completed.name):
        update_task_time(db, run_name, process, status)

    if type(error) is str:
        update_error(db, process, run_name, error)

    db.connection.commit()


def update_error(db, process, run_name, error):
    db.execute('''INSERT INTO error (task_id, error)
                  VALUES (
                  (SELECT id from state 
                   WHERE proc_type = (SELECT id FROM proc_type_enum WHERE proc_type = ?)
                    AND run_name = ?) ,
                  ?
                  )
                  ''', (process, run_name, error))


def update_task_time(db, run_name, process, status):
    db.execute('''INSERT OR IGNORE INTO
                  `task_time_log`(state_id, status, time)
                  VALUES((SELECT id FROM state_view WHERE run_name = ? AND proc_type = ? ),
                          (SELECT id FROM status_enum WHERE UPPER(state) = UPPER(?)),
                           strftime('%s','now'))''', (run_name, process, status))
    db.connection.commit()


def force_update_db(db, process, status, job=None, run_name=None, error='', retry=False, reset_retries=False):

    db.execute('''UPDATE state
                  SET status = (SELECT id FROM status_enum WHERE state = ?), 
                      last_modified = strftime('%s','now'),
                      job_id = ?
                  WHERE (job_id = ? or run_name = ?)
                   AND proc_type = (SELECT id FROM proc_type_enum WHERE proc_type = ?)
                   ''', (status, job, job, run_name, process))    
    if retry:
        db.execute('''UPDATE state
                      SET retries = retries + 1
                      WHERE (job_id = ? or run_name = ?)
                       AND proc_type = (SELECT id FROM proc_type_enum WHERE proc_type = ?)''', (job, run_name, process))
        update_error(db, process, run_name, 'Process failed retrying')
    if reset_retries:
        db.execute('''UPDATE state
                      SET retries = 0
                      WHERE (job_id = ? or run_name = ?)
                       AND proc_type = (SELECT id FROM proc_type_enum WHERE proc_type = ?)''', (job, run_name, process))
        update_error(db, process, run_name, 'Reseting retries')

    if type(error) is str:
        update_error(db, process, run_name, error)

    if run_name is not None and (status == db_helper.State.running or status == db_helper.State.completed):
        update_task_time(db, run_name, process, status)

    db.connection.commit()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('run_folder', type=str, 
                        help="folder to the collection of runs on Kupe")
    parser.add_argument('process', choices=db_helper.enum_to_list(db_helper.Process))
    parser.add_argument('status', type=str, choices=db_helper.enum_to_list(db_helper.State))
    parser.add_argument('-r', '--run_name', type=str,
                        help='name of run to be updated')
    parser.add_argument('-j', '--job', type=int)
    parser.add_argument('-e', '--error', type=str)
   
    parser.add_argument('-f', '--force', action="store_true")
    parser.add_argument('-rt', '--is_retry', action="store_true", default=False)
    parser.add_argument('-rr', '--reset_retries', action="store_true", default=False)
     
    args = parser.parse_args()
    f = args.run_folder
    process = args.process
    status = args.status
    run_name = args.run_name
    job_id = args.job
    error = args.error
    db = db_helper.connect_db(f)
    
    if args.force:
        force_update_db(db, process, status, job=job_id, run_name=run_name, error=error, retry=args.is_retry,
                        reset_retries=args.reset_retries)
    else:
        update_db(db, process, status, job=job_id, run_name=run_name, error=error)

    db.connection.commit()


if __name__ == '__main__':
    main()
