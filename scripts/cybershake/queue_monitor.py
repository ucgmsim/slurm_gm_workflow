#!/usr/bin/env python3
"""Script for continuously updating the slurm mgmt db from the queue."""
import signal
import os
import json
import argparse
import time
from logging import Logger
from typing import List
from datetime import datetime

import qcore.simulation_structure as sim_struct
from scripts.management.MgmtDB import MgmtDB, SlurmTask
from shared_workflow import workflow_logger


# Have to include sub-seconds, as clean up can run sub one second.
DATE_FORMAT = "%Y%m%d%H%M%S_%f"

QUEUE_MONITOR_LOG_FILE_NAME = "queue_monitor_log_{}.txt"

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
        retries=data_dict[MgmtDB.col_retries],
        error=data_dict.get("error"),
    )


def main(root_folder: str, sleep_time: int, queue_logger: Logger = workflow_logger.get_basic_logger()):
    mgmt_db = MgmtDB(sim_struct.get_mgmt_db(root_folder))
    queue_folder = sim_struct.get_mgmt_db_queue(root_folder)

    queue_logger.info("Running queue-monitor, exit with Ctrl-C.")

    sqlite_tmpdir = "/tmp/cer"
    while keepAlive:
        if not os.path.exists(sqlite_tmpdir):
            os.makedirs(sqlite_tmpdir)
            queue_logger.debug("Set up the sqlite_tmpdir")

        entry_files = os.listdir(queue_folder)
        entry_files.sort()

        entries = []
        for file in entry_files:
            entry = get_queue_entry(os.path.join(queue_folder, file), queue_logger)
            if entry is not None:
                entries.append(entry)
                os.remove(os.path.join(queue_folder, file))

        if len(entries) > 0:
            queue_logger.info("Updating {} mgmt db tasks_to_run.".format(len(entries)))
            if not mgmt_db.update_entries_live(entries, queue_logger):
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
    args = parser.parse_args()

    root_folder = os.path.abspath(args.root_folder)

    if args.log_file is None:
        log_file_name = os.path.join(
            args.root_folder,
            QUEUE_MONITOR_LOG_FILE_NAME.format(datetime.now().strftime(DATE_FORMAT)),
        )
    else:
        log_file_name = args.log_file

    workflow_logger.add_general_file_handler(logger, log_file_name)
    logger.debug("Successfully added {} as the log file.".format(log_file_name))

    signal.signal(signal.SIGINT, on_exit)
    main(root_folder, args.sleep_time, logger)
