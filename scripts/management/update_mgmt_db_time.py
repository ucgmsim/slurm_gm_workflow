"""
@author: jam335 - jason.motha@canterbury.ac.nz

A script that updates the submission time of a job in a slurm mgmt db
"""

import argparse
import db_helper


def update_job_time(db, job, status):
    db.execute('''INSERT OR IGNORE INTO
                  `job_time_log`(job_id, status, time)
                  VALUES(?,
                         (SELECT id FROM status_enum WHERE UPPER(state) = UPPER(?)),
                         strftime('%s','now'))''',
               (job, status))
    db.connection.commit()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('run_folder', type=str, 
                        help="folder to the collection of runs on Kupe")
    parser.add_argument('-j', '--job', type=int)
    parser.add_argument('status', type=str, choices=db_helper.enum_to_list(db_helper.State))
    
    args = parser.parse_args()
    f = args.run_folder
    db = db_helper.connect_db(f)
    status = args.status
    job_id = args.job
    
    update_job_time(db, job_id, status)

    db.connection.commit()


if __name__ == '__main__':
    main()
