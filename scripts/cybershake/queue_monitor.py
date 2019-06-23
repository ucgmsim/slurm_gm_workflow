#!/usr/bin/env python3
"""Script for continuously updating the slurm mgmt db from the queue."""
import logging
import signal
import os
import json
import argparse
import time
from logging import Logger
from typing import List, Dict
from datetime import datetime

import qcore.constants as const
import qcore.simulation_structure as sim_struct
from scripts.management.MgmtDB import MgmtDB, SlurmTask
from shared_workflow import workflow_logger
from shared_workflow.shared_automated_workflow import (
    get_queued_tasks,
    check_mgmt_queue,
)


# Have to include sub-seconds, as clean up can run sub one second.

QUEUE_MONITOR_LOG_FILE_NAME = "queue_monitor_log_{}.txt"
DEFAULT_N_MAX_RETRIES = 2

SLURM_TO_STATUS_DICT = {"R": 3, "PD": 2, "CG": 3}

logger = None
keepAlive = True


def on_exit(signum, frame):
    global logger
    if not logger:
        logger = workflow_logger.get_basic_logger()
    logger.critical("SIGINT recieved, exiting queue-monitor.")
    exit()


def get_queue_entry(
    entry_file: str, queue_logger: Logger = workflow_logger.get_basic_logger()
):
    try:
        with open(entry_file, "r") as f:
            data_dict = json.load(f)
    except json.JSONDecodeError as ex:
        queue_logger.error(
            "Failed to decode the file {} as json. Check that this is "
            "valid json. Ignored!".format(entry_file)
        )
        return None

    return SlurmTask(
        run_name=os.path.basename(entry_file).split(".")[1],
        proc_type=data_dict[MgmtDB.col_proc_type],
        status=data_dict[MgmtDB.col_status],
        job_id=data_dict[MgmtDB.col_job_id],
        error=data_dict.get("error"),
    )


def update_tasks(
    mgmt_queue_entries: List[str],
    squeue_tasks: Dict[str, str],
    db_running_tasks: List[SlurmTask],
    task_logger: Logger,
):
    """Updates the mgmt db entries based on the HPC queue"""
    tasks_to_do = []

    for db_running_task in db_running_tasks:
        if str(db_running_task.job_id) in squeue_tasks.keys():
            queue_status = squeue_tasks[str(db_running_task.job_id)]

            try:
                queue_status = SLURM_TO_STATUS_DICT[queue_status]
            except KeyError:
                task_logger.error(
                    "Failed to recogize state code {}, updating to {}".format(
                        queue_status, const.Status.unknown.value
                    )
                )
                queue_status = const.Status.unknown.value

            if queue_status == db_running_task.status:
                task_logger.debug(
                    "No need to update status {} for {}, {} ({}) as it "
                    "has not changed.".format(
                        const.Status(queue_status).str_value,
                        db_running_task.run_name,
                        const.ProcessType(db_running_task.proc_type).str_value,
                        db_running_task.job_id,
                    )
                )
            # Do nothing if there is a pending update for
            # this run & process type combination
            elif not check_mgmt_queue(
                mgmt_queue_entries, db_running_task.run_name, db_running_task.proc_type
            ):
                task_logger.info(
                    "Updating status of {}, {} from {} to {}".format(
                        db_running_task.run_name,
                        const.ProcessType(db_running_task.proc_type).str_value,
                        const.Status(db_running_task.status).str_value,
                        const.Status(queue_status).str_value,
                    )
                )
                tasks_to_do.append(
                    SlurmTask(
                        db_running_task.run_name,
                        db_running_task.proc_type,
                        queue_status,
                        None,
                        None,
                    )
                )
        # Only reset if there is no entry on the mgmt queue for this
        # realisation/proc combination and nothing in the mgmt folder
        elif not check_mgmt_queue(
            mgmt_queue_entries, db_running_task.run_name, db_running_task.proc_type
        ):
            task_logger.warning(
                "Task '{}' on '{}' not found on squeue; resetting the status "
                "to 'created' for resubmission".format(
                    const.ProcessType(db_running_task.proc_type).str_value,
                    db_running_task.run_name,
                )
            )
            # Add an error
            tasks_to_do.append(
                SlurmTask(
                    db_running_task.run_name,
                    db_running_task.proc_type,
                    const.Status.failed.value,
                    None,
                    "Disappeared from squeue. Creating a new task.",
                )
            )
    return tasks_to_do

  
def main(root_folder: str, sleep_time: int, max_retries: int, queue_logger: Logger = workflow_logger.get_basic_logger()):
    mgmt_db = MgmtDB(sim_struct.get_mgmt_db(root_folder))
    queue_folder = sim_struct.get_mgmt_db_queue(root_folder)

    queue_logger.info("Running queue-monitor, exit with Ctrl-C.")

    mgmt_db.add_retries(max_retries)

    sqlite_tmpdir = "/tmp/cer"
    while keepAlive:
        if not os.path.exists(sqlite_tmpdir):
            os.makedirs(sqlite_tmpdir)
            queue_logger.debug("Set up the sqlite_tmpdir")

        # For each hpc get a list of job id and status', and for each pair save them in a dictionary
        squeue_tasks = {
            task.split()[0]: task.split()[1]
            for hpc in const.HPC
            for task in get_queued_tasks(machine=hpc)
        }

        if len(squeue_tasks) > 0:
            queue_logger.info("Squeue user tasks: " + ", ".join([" ".join(task) for task in squeue_tasks.items()]))
        else:
            queue_logger.debug("No squeue user tasks")

        db_in_progress_tasks = mgmt_db.get_submitted_tasks()
        if len(db_in_progress_tasks) > 0:

            queue_logger.info(
                "In progress tasks in mgmt db:"
                + ", ".join(
                    [
                        "{}-{}-{}-{}".format(
                            entry.run_name,
                            const.ProcessType(entry.proc_type).str_value,
                            entry.job_id,
                            const.Status(entry.status).str_value,
                        )
                        for entry in db_in_progress_tasks
                    ]
                )
            )

        entry_files = os.listdir(queue_folder)
        entry_files.sort()

        entries = update_tasks(
            entry_files, squeue_tasks, db_in_progress_tasks, queue_logger
        )

        for file in entry_files:
            entry = get_queue_entry(os.path.join(queue_folder, file), queue_logger)
            if entry is not None:
                entries.append(entry)
                os.remove(os.path.join(queue_folder, file))

        if len(entries) > 0:
            queue_logger.info("Updating {} mgmt db tasks.".format(len(entries)))
            if not mgmt_db.update_entries_live(entries, max_retries, queue_logger):
                # Failed to update
                queue_logger.error(
                    "Failed to update the current entries in the mgmt db queue. "
                    "Please investigate and fix. If this is a repeating error, then this "
                    "will block all other entries from updating."
                )
        else:
            queue_logger.info("No entries in the mgmt db queue.")

        # Nap time
        queue_logger.debug("Sleeping for {}".format(sleep_time))
        time.sleep(sleep_time)


if __name__ == "__main__":
    logger = workflow_logger.get_logger()

    parser = argparse.ArgumentParser()

    parser.add_argument("root_folder", type=str, help="Cybershake root folder.")
    parser.add_argument(
        "--sleep_time",
        type=int,
        help="Sleep time (in seconds) between queue checks.",
        default=5,
    )
    parser.add_argument(
        "--log_file",
        type=str,
        default=None,
        help="Location of the log file to use. Defaults to 'cybershake_log.txt' in the location root_folder. "
        "Must be absolute or relative to the root_folder.",
    )
    parser.add_argument(
        "--n_max_retries",
        help="The maximum number of retries for any given task",
        default=DEFAULT_N_MAX_RETRIES,
        type=int,
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Print debug messages to stdout",
    )
    args = parser.parse_args()

    root_folder = os.path.abspath(args.root_folder)

    if args.log_file is None:
        log_file_name = os.path.join(
            args.root_folder,
            QUEUE_MONITOR_LOG_FILE_NAME.format(datetime.now().strftime(const.QUEUE_DATE_FORMAT)),
        )
    else:
        log_file_name = args.log_file

    if args.debug:
        workflow_logger.set_stdout_level(logger, logging.DEBUG)

    workflow_logger.add_general_file_handler(logger, log_file_name)
    logger.debug("Successfully added {} as the log file.".format(log_file_name))

    signal.signal(signal.SIGINT, on_exit)
    main(root_folder, args.sleep_time, args.n_max_retries, logger)
