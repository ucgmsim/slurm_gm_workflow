"""
@author: jam335 - jason.motha@canterbury.ac.nz

A script that queries slurm and updates the status of a task in a slurm db
"""

import argparse
import qcore.constants
from scripts.management import update_mgmt_db
from subprocess import Popen, PIPE
import shlex
from scripts.management import db_helper

CONSTANTS_TASK_TYPE_ = (x.str_value for x in qcore.constants.ProcessType)

Process = qcore.constants.ProcessType

N_TASKS_TO_RUN = 20
RETRY_MAX = 2

# TODO: Change the status strings to use the enum instead
# TODO: create task class instead of a 'list'

t_status = {"R": "running", "PD": "queued"}


def get_queued_tasks(user=None, machine=qcore.constants.HPC.maui):
    output_list = []
    # TODO: Treat Maui and Mahuika jobs seperately. See QSW-912
    if user is not None:
        cmd = "squeue -A nesi00213 -o '%A %t' -h -M {} -u {}".format(
            machine.value, user
        )
    else:
        cmd = "squeue -A nesi00213 -o '%A %t' -h -M {}".format(machine.value)
    process = Popen(shlex.split(cmd), stdout=PIPE, encoding="utf-8")
    (output, err) = process.communicate()
    exit_code = process.wait()
    output_list.extend(filter(None, output.split("\n")[1:]))
    return "\n".join(output_list)


def get_submitted_db_tasks(db):
    db.execute(
        """SELECT proc_type_enum.proc_type, run_name, job_id, status_enum.state 
                  FROM status_enum, proc_type_enum, state 
                  WHERE state.status = status_enum.id AND state.proc_type = proc_type_enum.id 
                   AND status_enum.state IN ('running', 'queued')"""
    )
    return db.fetchall()


def get_db_tasks_to_be_run(db, retry_max=RETRY_MAX):
    print("retry_max", retry_max)
    db.execute(
        """SELECT proc_type, run_name, status_enum.state 
                  FROM status_enum, state 
                  WHERE state.status = status_enum.id
                   AND ((status_enum.state = 'created' 
                         AND state.retries < ?)
                    OR status_enum.state = 'completed')""",
        (retry_max,),
    )
    return db.fetchall()


def update_tasks(db, tasks, db_tasks):
    for db_task in db_tasks:
        found = False
        proc_type, run_name, job_id, db_state = db_task
        for task in tasks.splitlines():
            #            print "db_task: ",db_task
            #            print "task:    ",task
            t_job_id, t_state = task.split()
            if job_id == int(t_job_id):
                found = True
                try:
                    t_state_str = t_status[t_state]
                except KeyError:
                    print("failed to recogize state code %s", t_state)
                    t_state_str = ""
                if t_state_str == db_state:
                    print(
                        "not updating status ({}) of '{}' on '{}' ({})".format(
                            t_state_str, proc_type, run_name, job_id
                        )
                    )
                else:
                    print(
                        "updating '{}' on '{}' to the status of '{}' from '{}' ({})".format(
                            proc_type, run_name, t_state_str, db_state, job_id
                        )
                    )
                    update_mgmt_db.update_db(
                        db, proc_type, t_state_str, job_id, run_name
                    )
        if not found:
            print(
                "Task '{}' on '{}' not found on squeue; resetting the status to 'created' for resubmission".format(
                    proc_type, run_name
                )
            )
            update_mgmt_db.force_update_db(
                db,
                proc_type,
                "created",
                job_id,
                run_name,
                error="Task removed from squeue without completion",
                retry=True,
            )
        db.connection.commit()


def is_task_complete(task, task_list):
    process, run_name, status = task
    for check_task in task_list:
        if (
            check_task[0] == process
            and check_task[1] == run_name
            and check_task[2] == status
        ):
            return True
    return False


def check_dependancy_met(task, task_list):
    process, run_name, status = task
    process = qcore.constants.ProcessType(process)
    if process in (Process.EMOD3D, Process.HF, Process.rrup):
        return True

    # If the process has completed the one linearly before it
    if process in (
        Process.merge_ts,
        Process.winbin_aio,
        Process.IM_calculation,
        Process.Empirical,
    ):
        dependant_task = list(task)
        dependant_task[0] = process.value - 1
        dependant_task[2] = "completed"
        return is_task_complete(dependant_task, task_list)

    if process is Process.BB:
        LF_task = list(task)
        LF_task[0] = Process.EMOD3D.value
        LF_task[2] = "completed"
        HF_task = list(task)
        HF_task[0] = Process.HF.value
        HF_task[2] = "completed"
        return is_task_complete(LF_task, task_list) and is_task_complete(
            HF_task, task_list
        )

    if process is Process.clean_up:
        IM_task = list(task)
        IM_task[0] = Process.IM_calculation.value
        IM_task[2] = "completed"
        merge_ts_task = list(task)
        merge_ts_task[0] = Process.merge_ts.value
        merge_ts_task[2] = "completed"
        return is_task_complete(IM_task, task_list) and is_task_complete(
            merge_ts_task, task_list
        )

    return False


def get_runnable_tasks(
    db, n_runs=None, retry_max=RETRY_MAX, task_types=CONSTANTS_TASK_TYPE_
):
    do_verification = False
    verification_tasks = [
        Process.rrup.value,
        Process.Empirical.value,
        Process.Verification.value,
    ]
    db_tasks = get_db_tasks_to_be_run(db, retry_max)
    tasks_to_run = []
    for task in db_tasks:
        status = task[2]
        if (
            status == "created"
            and check_dependancy_met(task, db_tasks)
            and task[0] in task_types
        ):
            if task[0] not in verification_tasks or do_verification:
                tasks_to_run.append(task)
        if n_runs is not None and len(tasks_to_run) >= n_runs:
            break

    return tasks_to_run


def run_new_task(db, n_runs):
    tasks = get_runnable_tasks(db, n_runs)
    if len(tasks) > 0:
        print(tasks[0], len(tasks))

        """####
            <insert code to run/submit a new task>
        ####"""

        return True
    else:
        return False


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "run_folder", type=str, help="folder to the collection of runs on Kupe"
    )
    parser.add_argument("--n_runs", "-n", default=N_TASKS_TO_RUN, type=int)

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


if __name__ == "__main__":
    main()
