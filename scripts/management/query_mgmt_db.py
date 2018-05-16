"""
@author: jam335 - jason.motha@canterbury.ac.nz

A script that queries a slurm mgmt db and returns the status of a task
"""

import argparse
import create_mgmt_db

def print_run_status(db, run_name, error=False):
    if error:
        db.execute('''SELECT state.run_name, proc_type_enum.proc_type, status_enum.state, datetime(last_modified,'unixepoch'), state.error
                    FROM state, status_enum, proc_type_enum
                    WHERE state.proc_type = proc_type_enum.id AND state.status = status_enum.id
                            AND state.run_name LIKE ? AND state.error <> ''
                    ORDER BY state.run_name, status_enum.id
                    ''', (run_name,))
        status = db.fetchall()
        for statum in status:
            print ''' Run_name: {}\n Process: {}\n Status: {}\n Last_Modified: {}\n Error: {} \n'''.format(*statum)
    else:
        db.execute('''SELECT state.run_name, proc_type_enum.proc_type, status_enum.state, datetime(last_modified,'unixepoch')
                    FROM state, status_enum, proc_type_enum
                    WHERE state.proc_type = proc_type_enum.id AND state.status = status_enum.id
                            AND state.run_name LIKE ?
                    ORDER BY state.run_name, status_enum.id
                    ''', (run_name,))
        status = db.fetchall()
        print "{:>25} | {:>15} | {:>10} | {:>20}".format('run_name', 'process', 'status', 'last_modified')
        print '_' * (25 + 15 + 10 + 20 + 3 * 3)
        for statum in status:
            print "{:>25} | {:>15} | {:>10} | {:>20}".format(*statum)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('run_folder', type=str, 
                        help="folder to the collection of runs on Kupe")
    parser.add_argument('run_name', type=str, nargs='?', default='%',
                        help='name of run to be queried')
    parser.add_argument('--error', '-e', default=None, action='store_true')
    
    args = parser.parse_args()
    f = args.run_folder
    run_name = args.run_name
    error = args.error
    db = create_mgmt_db.connect_db(f)
   
    print_run_status(db, run_name, error)
    
if __name__ == '__main__':
    main()
