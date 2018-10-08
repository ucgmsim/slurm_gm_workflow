"""
@author: jam335 - jason.motha@canterbury.ac.nz

A script that updates the submission time of a task in a slurm mgmt db
"""

import argparse
import db_helper


def update_time(db, run_name, process, status):
    db.execute('''INSERT OR IGNORE INTO
                  `time_log`(state_id, status, time)
                  VALUES((SELECT id FROM state_view WHERE run_name = ? AND proc_type = ? ),
                          (SELECT id FROM status_enum WHERE UPPER(state) = UPPER(?)),
                           strftime('%s','now'))''', (run_name, process, status))
    db.connection.commit()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('run_folder', type=str, 
                        help="folder to the collection of runs on Kupe")
    parser.add_argument('run_name', type=str,
                        help='name of run to be updated')
    parser.add_argument('process', choices=['EMOD3D', 'post_EMOD3D', 'HF', 'BB', 'IM_calculation'])
    parser.add_argument('status', type=str, choices=['created', 'in-queue', 'running', 'completed', 'failed'])
    
    args = parser.parse_args()
    f = args.run_folder
    process = args.process
    run_name = args.run_name
    db = db_helper.connect_db(f)
    status = args.status
    
    update_time(db, run_name, process, status)

    db.connection.commit()


if __name__ == '__main__':
    main()
