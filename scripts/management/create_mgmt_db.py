"""
@author: jam335 - jason.motha@canterbury.ac.nz

A script that creates a database and populates it with the status of 
each stage of the run
"""

#TODO: extract all db methods to a db module rather than importing scripts to use parts of them.

import argparse
import sqlite3
import os
import glob

def connect_db(path):
    db_location = os.path.join(path, 'slurm_mgmt.db')
    conn = sqlite3.connect(db_location)
    db = conn.cursor()
    db.execute("PRAGMA synchronous = OFF") 
    return db
    

def initilize_db(path):
    db = connect_db(path)
    sql_template_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), '../../templates/slurm_mgmt.db.sql')
    initilize_query = open(sql_template_file).read()
    db.executescript(initilize_query)
    db.connection.commit()
    return db


def get_procs(db):
    db.execute('''select * from proc_type_enum''')
    return db.fetchall()

def create_mgmt_db(realisations, f, srf_files=[]):
    additonal_realisations = [os.path.splitext(os.path.basename(srf))[0] for srf in srf_files]

    realisations.extend(additonal_realisations)

    if len(realisations) == 0:
        print "No realisations found - no entries inserted into db"

    db = initilize_db(f)    
    procs_to_be_done = get_procs(db)

    for run_name in realisations:
        for proc in procs_to_be_done:
            insert_task(db, run_name, proc[0])

    db.connection.commit()

def insert_task(db, run_name, proc):
    db.execute('''INSERT OR IGNORE INTO
                  `state`(run_name, proc_type, status, last_modified)
                  VALUES(?, ?, 1, strftime('%s','now'))''', (run_name, proc))
    db.connection.commit()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('run_folder', type=str, 
                        help="folder to the collection of runs on Kupe")
    parser.add_argument('realisations', type=str, nargs='+', help='space delimited list of realisations')
    
    args = parser.parse_args()
    f = args.run_folder
    realisations = args.realisations
    
    create_mgmt_db(realisations, f)


if __name__ == '__main__':
    main()
