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
    process = Popen(shlex.split(cmd), stdout=PIPE)
    (output, err) = process.communicate()
    exit_code = process.wait()

    return output

def get_submitted_db_tasks(db):
    db.execute('''SELECT proc_type_enum.proc_type, run_name, job_id, status_enum.state 
                  FROM status_enum, proc_type_enum, state 
                  WHERE state.status = status_enum.id AND state.proc_type = proc_type_enum.id 
                   AND status_enum.state IN ('running', 'in-queue')''')
    return db.fetchall()

def get_db_tasks_to_be_run(db):
    db.execute('''SELECT proc_type, run_name, status_enum.state 
                  FROM status_enum, state 
                  WHERE state.status = status_enum.id
                   AND status_enum.state IN ('created', 'completed') ''')
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

def is_task_complete(task, task_list):
    process, run_name, status = task
    for check_task in task_list:
        if check_task[0] == process and check_task[1] == run_name and check_task[2] == status:
            return True
    return False

def check_dependancy_met(task, task_list):
    process, run_name, status = task
    if process is 1 or process is 4: # EMOD & HF
        return True
    if process in (2, 3, 6): # post EMOD & IM_processing
        dependant_task = list(task)
        dependant_task[0] = process - 1
        dependant_task[2] = 'completed'
        return is_task_complete(dependant_task, task_list)
    if process is 5: # BB
        LF_task = list(task)
        LF_task[0] = 3
        LF_task[2] = 'completed'
        HF_task = list(task)
        HF_task[0] = 4
        HF_task[2] = 'completed'
        return is_task_complete(LF_task, task_list) and is_task_complete(HF_task, task_list)

def get_runnable_tasks(db):
    db_tasks = get_db_tasks_to_be_run(db)
    tasks_to_run = []
    for task in db_tasks:
        status = task[2]
        if status == 'created' and check_dependancy_met(task, db_tasks):
            tasks_to_run.append(task)
                
    return tasks_to_run


def run_new_task(db):
    tasks = get_runnable_tasks(db)
    if len(tasks) > 0:
        print tasks[0]
        
        """####
            <insert code to run/submit a new task>
        ####"""

        return True
    else:
        return False

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('run_folder', type=str, 
                        help="folder to the collection of runs on Kupe")
    parser.add_argument('poll_interval', type=int, default=180, nargs='?')
    parser.add_argument('--n_runs', '-n', default=20, type=int)
    
    args = parser.parse_args()
    f = args.run_folder
    poll_interval = args.poll_interval
    n_runs = args.n_runs
    db = create_mgmt_db.connect_db(f)
    db_tasks = []

    tasks = get_queued_tasks()
    db_tasks = get_submitted_db_tasks(db)
    update_tasks(db, tasks, db_tasks)

    tasks_to_run = True

    while len(db_tasks) < n_runs and tasks_to_run is True:
        tasks_to_run = run_new_task(db)
        
        


    db.connection.commit()

if __name__ == '__main__':
    main()
