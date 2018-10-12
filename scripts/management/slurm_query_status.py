"""
@author: jam335 - jason.motha@canterbury.ac.nz

A script that queries slurm and updates the status of a task in a slurm db
"""

import argparse
import update_mgmt_db
from subprocess import Popen, PIPE
import shlex
import db_helper
from db_helper import Process

N_TASKS_TO_RUN = 20
RETRY_MAX = 2

# TODO: Change the status strings to use the enum instead
# TODO: create task class instead of a 'list'

t_status = {'R': 'running', 'PD': 'queued'}


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
                   AND status_enum.state IN ('running', 'queued')''')
    return db.fetchall()


def get_db_tasks_to_be_run(db, retry_max=RETRY_MAX):
    db.execute('''SELECT proc_type, run_name, status_enum.state 
                  FROM status_enum, state 
                  WHERE state.status = status_enum.id
                   AND status_enum.state IN ('created', 'completed') 
                   AND state.retries < ?''', (retry_max,))
    return db.fetchall()


def update_tasks(db, tasks, db_tasks):
    for db_task in db_tasks:
        found = False
        proc_type, run_name, job_id, db_state = db_task
        for task in tasks.splitlines():
            t_job_id, t_state = task.split()
            if job_id == int(t_job_id):
                found = True
                try:
                    t_state_str = t_status[t_state]
                except KeyError:
                    print "failed to recogize state code %s",t_state
                    t_state_str == ''
                if t_state_str == db_state:
                    print "not updating status ({}) of '{}' on '{}' ({})".format(t_state_str, proc_type, run_name, job_id)
                else:
                    print "updating '{}' on '{}' to the status of '{}' from '{}' ({})".format(proc_type, run_name, t_state_str, db_state, job_id)
                    update_mgmt_db.update_db(db, proc_type, t_state_str, job_id, run_name)
        if not found:
            print "Task '{}' on '{}' not found on squeue; resetting the status to 'created' for resubmission".format(proc_type, run_name)
            update_mgmt_db.force_update_db(db, proc_type, 'created', job_id, run_name, error='Task removed from squeue without completion', retry=True)
        db.connection.commit()


def is_task_complete(task, task_list):
    process, run_name, status = task
    for check_task in task_list:
        if check_task[0] == process and check_task[1] == run_name and check_task[2] == status:
            return True
    return False


def check_dependancy_met(task, task_list):
    process, run_name, status = task
    process = db_helper.Process(process)
    if process is Process.EMOD3D or process is Process.HF:
        return True

    if process in (Process.merge_ts, Process.winbin_aio, Process.IM_calculation):
        dependant_task = list(task)
        dependant_task[0] = process.value - 1
        dependant_task[2] = 'completed'
        return is_task_complete(dependant_task, task_list)

    if process is Process.BB:
        LF_task = list(task)
        LF_task[0] = Process.winbin_aio.value
        LF_task[2] = 'completed'
        HF_task = list(task)
        HF_task[0] = Process.HF.value
        HF_task[2] = 'completed'
        return is_task_complete(LF_task, task_list) and is_task_complete(HF_task, task_list)
    return False


def get_runnable_tasks(db, n_runs=N_TASKS_TO_RUN, retry_max=RETRY_MAX):
    db_tasks = get_db_tasks_to_be_run(db, retry_max)
    tasks_to_run = []
    for task in db_tasks:
        status = task[2]
        if status == 'created' and check_dependancy_met(task, db_tasks):
            tasks_to_run.append(task)
        if len(tasks_to_run) >= n_runs:
            break
                
    return tasks_to_run


def run_new_task(db, n_runs):
    tasks = get_runnable_tasks(db, n_runs)
    if len(tasks) > 0:
        print tasks[0], len(tasks)
        
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
    parser.add_argument('--n_runs', '-n', default=N_TASKS_TO_RUN, type=int)
    
    args = parser.parse_args()
    f = args.run_folder
    poll_interval = args.poll_interval
    n_runs = args.n_runs
    db = db_helper.connect_db(f)
    db_tasks = []

    tasks = get_queued_tasks()
    db_tasks = get_submitted_db_tasks(db)
    update_tasks(db, tasks, db_tasks)

    tasks_to_run = run_new_task(db, n_runs)
        
    db.connection.commit()


if __name__ == '__main__':
    main()
