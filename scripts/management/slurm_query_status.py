"""
@author: jam335 - jason.motha@canterbury.ac.nz

A script that queries slurm and updates the status of a task in a slurm db
"""

import argparse
import create_mgmt_db
import update_mgmt_db
from subprocess import Popen, PIPE
import shlex

t_status = {'R': 'running', 'PD': 'in-queue'}

def get_queued_tasks():
    cmd = "squeue -A nesi00213 -o '%A %t' -h"
    cmd = '''echo "2182900 R
2183326 R
2183303 R
2183228 R
2183320 R
2183265 R
2183096 R
2183264 R
2183316 R
2183321 R
2183323 R
2183331 R
2183338 R
2183253 R
2183255 PD
2183280 PD
2183296 PD
2183324 PD
2183339 PD"'''
    process = Popen(shlex.split(cmd), stdout=PIPE)
    (output, err) = process.communicate()
    exit_code = process.wait()

    return output

def get_db_tasks(db):
    db.execute('''SELECT proc_type_enum.proc_type, run_name, job_id, status_enum.state 
                  FROM status_enum, proc_type_enum, state 
                  WHERE state.status = status_enum.id AND state.proc_type = proc_type_enum.id 
                   AND status_enum.state IN ('running', 'in-queue')''')
    return db.fetchall()

def update_tasks(db, tasks, db_tasks):
    for db_task in db_tasks:
        found = False
        proc_type, run_name, job_id, db_state = db_task
        for task in tasks.splitlines():
            t_job_id, t_state = task.split()
            if job_id == int(t_job_id):
                found = True
                t_state_str = t_status[t_state]
                if t_state_str == db_state:
                    print "not updating status ({}) of '{}' on '{}' ({})".format(t_state_str, proc_type, run_name, job_id)
                else:
                    print "updating '{}' on '{}' to the status of '{}' from '{}' ({})".format(proc_type, run_name, t_state_str, db_state, job_id)
                    update_mgmt_db.update_db(db, proc_type, t_state_str, job_id, run_name)
        if not found:
            print "Task '{}' on '{}' not found on squeue; changing status to 'failed'".format(proc_type, run_name)
            update_mgmt_db.update_db(db, proc_type, 'failed', job_id, run_name, error='Task removed from squeue without completion')
            

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('run_folder', type=str, 
                        help="folder to the collection of runs on Kupe")
    parser.add_argument('poll_interval', type=int, default=180, nargs='?')
    
    args = parser.parse_args()
    f = args.run_folder
    poll_interval = args.poll_interval
    db = create_mgmt_db.connect_db(f)
    
    tasks = get_queued_tasks()
    db_tasks = get_db_tasks(db)
    update_tasks(db, tasks, db_tasks)

    db.connection.commit()

if __name__ == '__main__':
    main()
