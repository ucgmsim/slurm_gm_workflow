"""
@author: jam335 - jason.motha@canterbury.ac.nz

A script that creates a database and populates it with the status of 
each stage of the run
"""
import argparse
import os

from scripts.management.db_helper import connect_db


def initilize_db(path):
    db = connect_db(path)
    sql_template_file = os.path.join(
        os.path.dirname(os.path.realpath(__file__)), 'slurm_mgmt.db.sql')
    initilize_query = open(sql_template_file).read()
    db.executescript(initilize_query)
    db.connection.commit()
    return db


def get_procs(db):
    db.execute('''select * from proc_type_enum''')
    return db.fetchall()


def create_mgmt_db(realisations, f, srf_files=[]):
    # for manual install, only one srf will be passed to srf_files as a string
    if isinstance(srf_files, str):
        srf_files = [srf_files]

    additonal_realisations = [os.path.splitext(os.path.basename(srf))[0] for srf in srf_files]
    realisations.extend(additonal_realisations)

    if len(realisations) == 0:
        print("No realisations found - no entries inserted into db")

    db = initilize_db(f)    
    procs_to_be_done = get_procs(db)

    for run_name in realisations:
        for proc in procs_to_be_done:
            insert_task(db, run_name, proc[0])

    db.connection.commit()
    return db

def insert_task(db, run_name, proc):
    db.execute('''INSERT OR IGNORE INTO
                  `state`(run_name, proc_type, status, last_modified, retries)
                  VALUES(?, ?, 1, strftime('%s','now'), 0)''', (run_name, proc))
    db.connection.commit()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('run_folder', type=str, 
                        help="folder to the collection of runs on Kupe")
    parser.add_argument('realisations', type=str, nargs='+',
                        help='space delimited list of realisations')
    
    args = parser.parse_args()
    f = args.run_folder
    realisations = args.realisations
    
    db = create_mgmt_db(realisations, f)
    db.connection.close()
    

if __name__ == '__main__':
    main()
