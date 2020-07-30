"""
Shared functions only used by the automated workflow
"""
import json
import os
from datetime import datetime
from logging import Logger
from typing import List

import qcore.constants as const
from qcore.utils import load_yaml
from scripts.management.MgmtDB import MgmtDB
from qcore.qclogging import get_basic_logger, NOPRINTCRITICAL
from scripts.schedulers.scheduler_factory import Scheduler

ALL = "ALL"
ONCE = "ONCE"
ONCE_PATTERN = "%_REL01"
NONE = "NONE"


def submit_script_to_scheduler(
    script: str,
    proc_type: int,
    queue_folder: str,
    sim_dir: str,
    run_name: str,
    target_machine: str = None,
    logger: Logger = get_basic_logger(),
):
    """
    Submits the slurm script and updates the management db.
    Calling the scheduler submitter may result in an error being raised.
    This is not caught in order to get immediate attention of broken runs.
    :param sim_dir:
    :param script: The location of the script to be run
    :param proc_type: The process type of the job being run
    :param queue_folder: Where the folder for database updates is
    :param run_name: The name of the realisation
    :param target_machine: The
    :param logger:
    :return:
    """
    job_id = Scheduler.get_scheduler().submit_job(sim_dir, script, target_machine)

    add_to_queue(
        queue_folder,
        run_name,
        proc_type,
        const.Status.queued.value,
        job_id=job_id,
        logger=logger,
    )


def add_to_queue(
    queue_folder: str,
    run_name: str,
    proc_type: int,
    status: int,
    job_id: int = None,
    error: str = None,
    logger: Logger = get_basic_logger(),
):
    """Adds an update entry to the queue"""
    logger.debug(
        "Adding task to the queue. Realisation: {}, process type: {}, status: {}, job_id: {}, error: {}".format(
            run_name, proc_type, status, job_id, error
        )
    )
    filename = os.path.join(
        queue_folder,
        "{}.{}.{}".format(
            datetime.now().strftime(const.QUEUE_DATE_FORMAT), run_name, proc_type
        ),
    )

    if os.path.exists(filename):
        logger.log(
            NOPRINTCRITICAL,
            "An update with the name {} already exists. This should never happen. Quitting!".format(
                os.path.basename(filename)
            ),
        )
        raise Exception(
            "An update with the name {} already exists. This should never happen. Quitting!".format(
                os.path.basename(filename)
            )
        )

    logger.debug("Writing update file to {}".format(filename))

    with open(filename, "w") as f:
        json.dump(
            {
                MgmtDB.col_run_name: run_name,
                MgmtDB.col_proc_type: proc_type,
                MgmtDB.col_status: status,
                MgmtDB.col_job_id: job_id,
                "error": error,
            },
            f,
        )

    if not os.path.isfile(filename):
        logger.critical("File {} did not successfully write".format(filename))
    else:
        logger.debug("Successfully wrote task update file")


def check_mgmt_queue(
    queue_entries: List[str], run_name: str, proc_type: int, logger=get_basic_logger()
):
    """Returns True if there are any queued entries for this run_name and process type,
    otherwise returns False.
    """
    logger.debug(
        "Checking to see if the realisation {} has a process of type {} in updates folder".format(
            run_name, proc_type
        )
    )
    for entry in queue_entries:
        logger.debug("Checking against {}".format(entry))
        _, entry_run_name, entry_proc_type = entry.split(".")
        if entry_run_name == run_name and entry_proc_type == str(proc_type):
            logger.debug("It's a match, returning True")
            return True
    logger.debug("No match found")
    return False


def parse_config_file(config_file_location: str, logger: Logger = get_basic_logger()):
    """Takes in the location of a wrapper config file and creates the tasks to be run.
    Each task that is desired to be run should have its name as given in qcore.constants followed by the relevant
    keyword or sqlite formatted query string, which uses % as the wildcard character.
    The keywords NONE, ONCE and ALL correspond to the patterns nothing, "%_REL01", "%" respectively.
    :param config_file_location: The location of the config file
    :param logger: The logger object used to record messages
    :return: A list containing the tasks to be run on all processes and a dictionary of pattern, task list pairs which
    state which query patterns should run which tasks
    """
    config = load_yaml(config_file_location)

    tasks_to_run_for_all = []
    tasks_with_pattern_match = {}

    for proc_name, pattern in config.items():
        proc = const.ProcessType.from_str(proc_name)
        if pattern == ALL:
            tasks_to_run_for_all.append(proc)
        elif pattern == NONE:
            pass
        else:
            if pattern == ONCE:
                pattern = ONCE_PATTERN
            if pattern not in tasks_with_pattern_match.keys():
                tasks_with_pattern_match.update({pattern: []})
            tasks_with_pattern_match[pattern].append(proc)
    logger.info("Master script will run {}".format(tasks_to_run_for_all))
    for pattern, tasks in tasks_with_pattern_match.items():
        logger.info("Pattern {} will run tasks {}".format(pattern, tasks))

    return tasks_to_run_for_all, tasks_with_pattern_match.items()
