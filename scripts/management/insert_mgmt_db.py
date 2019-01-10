"""
@author: jam335 - jason.motha@canterbury.ac.nz

A script that updates a slurm mgmt db and inserts a new task to run
"""

import argparse
import create_mgmt_db
import scripts.management.db_helper


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('run_folder', type=str, 
                        help="folder to the collection of runs on Kupe")
    parser.add_argument('run_name', type=str,
                        help='name of run to be updated')
    parser.add_argument('process', choices=['EMOD3D', 'post_EMOD3D', 'HF', 'BB', 'IM_calculation'])
    
    args = parser.parse_args()
    f = args.run_folder
    process = args.process
    run_name = args.run_name
    db = scripts.management.db_helper.connect_db(f)
    
    create_mgmt_db.insert_task(db, run_name, process)

    db.connection.commit()
    db.connection.close()

if __name__ == '__main__':
    main()
