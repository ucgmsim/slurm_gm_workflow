"""
Shared functions only used by the automated workflow
"""
import json
import os
from datetime import datetime
from logging import Logger
from typing import List

import qcore.constants as const
from qcore import utils as qc_utils
from qcore import qclogging

from workflow.automation.lib.MgmtDB import MgmtDB
from workflow.automation.lib.schedulers.scheduler_factory import Scheduler

ALL = "ALL"
MEDIAN_ONLY = "MEDIAN"
# MEDIAN_ONLY_PATTERN is negation of REL_ONLY_PATTERN. Has to be done in SQL
REL_ONLY = "REL_ONLY"
REL_ONLY_PATTERN = "%_REL%"
NONE = "NONE"


def submit_script_to_scheduler(
    script: str,
    proc_type: int,
    queue_folder: str,
    sim_dir: str,
    run_name: str,
    target_machine: str = None,
    logger: Logger = qclogging.get_basic_logger(),
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
    start_time: int = None,
    end_time: int = None,
    nodes: int = None,
    cores: int = None,
    memory: int = None,
    wct: int = None,
    logger: Logger = qclogging.get_basic_logger(),
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
            qclogging.NOPRINTCRITICAL,
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
                MgmtDB.col_queued_time: int(datetime.now().timestamp()),
                MgmtDB.col_start_time: start_time,
                MgmtDB.col_end_time: end_time,
                MgmtDB.col_nodes: nodes,
                MgmtDB.col_cores: cores,
                MgmtDB.col_memory: memory,
                MgmtDB.col_wct: wct,
            },
            f,
        )

    if not os.path.isfile(filename):
        logger.critical("File {} did not successfully write".format(filename))
    else:
        logger.debug("Successfully wrote task update file")


def check_mgmt_queue(
    queue_entries: List[str],
    run_name: str,
    proc_type: int,
    logger=qclogging.get_basic_logger(),
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


def parse_config_file(task_config: str, logger: Logger = qclogging.get_basic_logger()):
    """Takes in the location of a wrapper config file and creates the tasks to be run.
    Requires that the file contains the keys 'run_all_tasks' and 'run_some', even if they are empty
    If the dependencies for a run_some task overlap with those in the tasks_to_run_for_all, as a race condition is
    possible if multiple auto_submit scripts have the same tasks. If multiple run_some instances have the same
    dependencies then this is not an issue as they run sequentially, rather than simultaneously
    :param config_file: The location of the config file
    :return: A tuple containing the tasks to be run on all processes and a list of pattern, tasks tuples which state
    which tasks can be run with which patterns
    """
    if isinstance(task_config, str):
        config = qc_utils.load_yaml(task_config)
    else:
        config = task_config

    tasks_to_run_for_all = []
    tasks_with_pattern_match = {}
    tasks_with_anti_pattern_match = {}

    for proc_name, pattern in config.items():
        proc = const.ProcessType.from_str(proc_name)
        if pattern == ALL or ALL in pattern:
            tasks_to_run_for_all.append(proc)
            # If something has ALL it should only be added to the main runner and no other
            continue
        if isinstance(pattern, str):
            pattern = [pattern]
        for subpattern in pattern:
            if subpattern == REL_ONLY:
                add_to_dict_list(proc, tasks_with_pattern_match)
            elif subpattern == MEDIAN_ONLY:
                add_to_dict_list(proc, tasks_with_anti_pattern_match)
            elif subpattern == NONE:
                pass
            else:
                add_to_dict_list(proc, tasks_with_pattern_match, subpattern)
    logger.info("Master script will run {}".format(tasks_to_run_for_all))
    for pattern, tasks in tasks_with_pattern_match.items():
        logger.info("Pattern {} will run tasks {}".format(pattern, tasks))

    return (
        tasks_to_run_for_all,
        tasks_with_pattern_match.items(),
        tasks_with_anti_pattern_match.items(),
    )


def add_to_dict_list(proc_to_add, dict_to_add_to, pattern=REL_ONLY_PATTERN):
    if pattern not in dict_to_add_to:
        dict_to_add_to.update({pattern: []})
    dict_to_add_to[pattern].append(proc_to_add)
