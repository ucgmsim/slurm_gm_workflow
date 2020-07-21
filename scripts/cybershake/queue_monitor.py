#!/usr/bin/env python3
"""Script for continuously updating the slurm mgmt db from the queue."""
import logging
import os
import json
import urllib
import argparse
import time
from logging import Logger
from typing import List, Dict
from datetime import datetime

from qcore.qclogging import VERYVERBOSE

import qcore.constants as const
import qcore.simulation_structure as sim_struct
from qcore import qclogging
from scripts.management.MgmtDB import MgmtDB, SchedulerTask
from scripts.schedulers.scheduler_factory import get_scheduler
from shared_workflow.platform_config import HPC
from shared_workflow.shared_automated_workflow import check_mgmt_queue
from metadata.log_metadata import store_metadata

# Have to include sub-seconds, as clean up can run sub one second.

QUEUE_MONITOR_LOG_FILE_NAME = "queue_monitor_log_{}.txt"
DEFAULT_N_MAX_RETRIES = 2

keepAlive = True


def send_alert(msg, alert_url):
    data = {"text": msg}

    req = urllib.request.Request(alert_url)
    # create header and data, slack-bot requires json data
    req.add_header("Content-Type", "application/json; charset=utf-8")
    jsondata = json.dumps(data)
    jsondataasbytes = jsondata.encode("utf-8")
    req.add_header("Content-Length", len(jsondataasbytes))
    response = urllib.request.urlopen(req, jsondataasbytes)


def get_queue_entry(
    entry_file: str, queue_logger: Logger = qclogging.get_basic_logger()
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

    return SchedulerTask(
        run_name=os.path.basename(entry_file).split(".")[1],
        proc_type=data_dict[MgmtDB.col_proc_type],
        status=data_dict[MgmtDB.col_status],
        job_id=data_dict[MgmtDB.col_job_id],
        error=data_dict.get("error"),
    )


def update_tasks(
    mgmt_queue_entries: List[str],
    squeue_tasks: Dict[str, str],
    db_running_tasks: List[SchedulerTask],
    complete_data: bool,
    task_logger: Logger,
    root_folder: str,
):
    """Updates the mgmt db entries based on the HPC queue"""
    tasks_to_do = []

    task_logger.debug("Checking running tasks in the db for updates")
    task_logger.debug(
        f"The key value pairs found in {get_scheduler().QUEUE_NAME} are as follows: {squeue_tasks.items()}"
    )
    for db_running_task in db_running_tasks:
        task_logger.debug("Checking task {}".format(db_running_task))
        if str(db_running_task.job_id) in squeue_tasks.keys():

            queue_status = squeue_tasks[str(db_running_task.job_id)]
            task_logger.debug("Found task. It has state {}".format(queue_status))

            try:
                queue_status = get_scheduler().STATUS_DICT[queue_status]
            except KeyError:
                task_logger.error(
                    "Failed to recognize state code {}, updating to {}".format(
                        queue_status, const.Status.unknown.value
                    )
                )
                queue_status = const.Status.unknown.value
            task_logger.debug("This state represents status {}".format(queue_status))

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
                mgmt_queue_entries,
                db_running_task.run_name,
                db_running_task.proc_type,
                logger=task_logger,
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
                    SchedulerTask(
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
            mgmt_queue_entries,
            db_running_task.run_name,
            db_running_task.proc_type,
            logger=task_logger,
        ):
            if not complete_data:
                task_logger.warning(
                    f"Task '{const.ProcessType(db_running_task.proc_type).str_value}' not found on "
                    f"{get_scheduler().QUEUE_NAME} or in the management db folder, "
                    f"but errors were encountered when querying {get_scheduler().QUEUE_NAME}. Not resubmitting."
                )
            else:
                task_logger.warning(
                    f"Task '{const.ProcessType(db_running_task.proc_type).str_value}' on '{db_running_task.run_name}' "
                    f"not found on {get_scheduler().QUEUE_NAME} or in the management db folder; resetting the status "
                    "to 'created' for resubmission"
                )
                # Add an error
                tasks_to_do.append(
                    SchedulerTask(
                        db_running_task.run_name,
                        db_running_task.proc_type,
                        const.Status.failed.value,
                        None,
                        f"Disappeared from {get_scheduler().QUEUE_NAME}. Creating a new task.",
                    )
                )
            # When job failed, we want to log metadata as well
            (
                start_time,
                end_time,
                run_time,
                n_cores,
                status,
            ) = get_scheduler().get_metadata(db_running_task, task_logger)
            log_file = os.path.join(
                sim_struct.get_sim_dir(root_folder, db_running_task.run_name),
                "ch_log",
                "metadata_log.json",
            )
            # now log metadata
            store_metadata(
                log_file,
                const.ProcessType(db_running_task.proc_type).str_value,
                {
                    "start_time": start_time,
                    "end_time": end_time,
                    "run_time": run_time,
                    "cores": n_cores,
                    "status": status,
                },
                logger=task_logger,
            )
    return tasks_to_do


def queue_monitor_loop(
    root_folder: str,
    sleep_time: int,
    max_retries: int,
    queue_logger: Logger = qclogging.get_basic_logger(),
    alert_url=None,
):
    mgmt_db = MgmtDB(sim_struct.get_mgmt_db(root_folder))
    queue_folder = sim_struct.get_mgmt_db_queue(root_folder)

    queue_logger.info("Running queue-monitor, exit with Ctrl-C.")

    mgmt_db.add_retries(max_retries)

    sqlite_tmpdir = "/tmp/cer"
    while keepAlive:
        complete_data = True
        if not os.path.exists(sqlite_tmpdir):
            os.makedirs(sqlite_tmpdir)
            queue_logger.debug("Set up the sqlite_tmpdir")

        # For each hpc get a list of job id and status', and for each pair save them in a dictionary
        queued_tasks = {}
        for hpc in HPC:
            try:
                squeued_tasks = get_scheduler().check_queues(
                    user=None, target_machine=hpc
                )
            except EnvironmentError as e:
                queue_logger.critical(e)
                queue_logger.critical(
                    f"An error was encountered when attempting to check {get_scheduler().QUEUE_NAME} for HPC {hpc}. "
                    "Tasks will not be submitted to this HPC until the issue is resolved"
                )
                complete_data = False
            else:
                for task in squeued_tasks:
                    queued_tasks[task.split()[0]] = task.split()[1]

        if len(queued_tasks) > 0:
            if len(queued_tasks) > 200:
                queue_logger.log(
                    VERYVERBOSE,
                    f"{get_scheduler().QUEUE_NAME} tasks: {', '.join([' '.join(task) for task in queued_tasks.items()])}",
                )
                queue_logger.info(
                    f"Over 200 tasks were found in the queue. Check the log for an exact listing of them"
                )
            else:
                queue_logger.info(
                    f"{get_scheduler().QUEUE_NAME} tasks: {', '.join([' '.join(task) for task in queued_tasks.items()])}"
                )
        else:
            queue_logger.debug(f"No {get_scheduler().QUEUE_NAME} tasks")

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

        entries = []

        for file_name in entry_files[::-1]:
            queue_logger.debug(
                "Checking {} to see if it is a valid update file".format(file_name)
            )
            entry = get_queue_entry(os.path.join(queue_folder, file_name), queue_logger)
            if entry is None:
                queue_logger.debug(
                    "Removing {} from the list of update files".format(file_name)
                )
                entry_files.remove(file_name)
            else:
                if str(entry.job_id) in queued_tasks.keys() and entry.status > 3:
                    # This will prevent race conditions if the failure/completion state file is made and picked up before the job actually finishes
                    # Most notabley happens on Kisti
                    # The queued and running states are allowed
                    queue_logger.debug(
                        "Job {} is still running on the HPC, skipping this iteration".format(
                            entry
                        )
                    )
                    entry_files.remove(file_name)
                else:
                    queue_logger.debug("Adding {} to the list of updates".format(entry))
                    entries.insert(0, entry)

        entries.extend(
            update_tasks(
                entry_files,
                queued_tasks,
                db_in_progress_tasks,
                complete_data,
                queue_logger,
                root_folder,
            )
        )

        if len(entries) > 0:
            queue_logger.info("Updating {} mgmt db tasks.".format(len(entries)))
            if mgmt_db.update_entries_live(entries, max_retries, queue_logger):
                for file_name in entry_files:
                    os.remove(os.path.join(queue_folder, file_name))
                # check for jobs that matches alert criteria
                if alert_url != None:
                    for entry in entries:
                        if entry.status == const.Status.failed.value:
                            entry_retries = mgmt_db.get_retries(
                                entry.proc_type, entry.run_name
                            )
                            if entry_retries < max_retries:
                                msg = f"fault:{entry.run_name} step:{entry.proc_type} has failed with error:{entry.error}"
                            elif entry_retries >= max_retries:
                                msg = f"@here fault:{entry.run_name} step:{entry.proc_type} has failed with error:{entry.error} and met the retry cap"
                            send_alert(msg, alert_url)
            else:
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


def initialisation():
    logger = qclogging.get_logger()

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
        "--debug", action="store_true", help="Print debug messages to stdout"
    )
    parser.add_argument(
        "--alert_url", help="the url to slack alert channel", default=None
    )
    args = parser.parse_args()

    root_folder = os.path.abspath(args.root_folder)

    if args.log_file is None:
        log_file_name = os.path.join(
            args.root_folder,
            QUEUE_MONITOR_LOG_FILE_NAME.format(
                datetime.now().strftime(const.QUEUE_DATE_FORMAT)
            ),
        )
    else:
        log_file_name = args.log_file

    if args.debug:
        qclogging.set_stdout_level(logger, logging.DEBUG)

    qclogging.add_general_file_handler(logger, log_file_name)
    logger.debug("Successfully added {} as the log file.".format(log_file_name))

    queue_monitor_loop(
        root_folder,
        args.sleep_time,
        args.n_max_retries,
        logger,
        alert_url=args.alert_url,
    )


if __name__ == "__main__":
    initialisation()
