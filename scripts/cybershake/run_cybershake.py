from datetime import datetime
from logging import Logger, DEBUG
from os.path import abspath, join
import threading
import argparse
from typing import Dict, List, Tuple

from qcore.config import platform_config, HPC
from qcore import constants as const
from qcore import qclogging
from qcore.utils import load_yaml

import estimation.estimate_wct as est
from scripts.cybershake import queue_monitor
from scripts.cybershake.auto_submit import run_main_submit_loop
from scripts.schedulers.scheduler_factory import initialise_scheduler

MASTER_LOG_NAME = "master_log_{}.txt"
SCHEDULER_LOG_NAME = "scheduler_log_{}.txt"
WRAPPER_LOG_FILE_NAME = "wrapper_log_{}.txt"
QUEUE_MONITOR_LOG_FILE_NAME = "queue_monitor_log_{}.txt"
MASTER_AUTO_SUBMIT_LOG_FILE_NAME = "main_auto_submit_log_{}.txt"
PATTERN_AUTO_SUBMIT_LOG_FILE_NAME = "pattern_{}_auto_submit_log_{}.txt"

ALL = "ALL"
ONCE = "ONCE"
ONCE_PATTERN = "%_REL01"
NONE = "NONE"


def run_automated_workflow(
    root_folder: str,
    log_directory: str,
    n_runs: Dict[str, int],
    n_max_retries: int,
    tasks_to_run: List[const.ProcessType],
    sleep_time: int,
    tasks_to_run_with_pattern: List[Tuple[str, List[const.ProcessType]]],
    wrapper_logger: Logger,
    debug: bool,
    alert_url=None,
):
    """Runs the automated workflow. Beings the queue monitor script and the script for tasks that apply to all
    realisations. Then while the all realisation thread is running go through each pattern and run all tasks that are
    available. When each instance of auto_submit doesn't submit anything or have anything running for an iteration it
    will automatically return, and the next pattern will have its tasks automatically submitted.
    It is advised that each task list within tasks_to_run_with_pattern be disjoint from task_types_to_run as a race condition
    may occur, and the task run twice at the same time, resulting in file writing issues.
    :param root_folder: The root directory of the cybershake folder structure
    :param log_directory: The directory the log files are to be placed in
    :param n_runs: The maximum number of processes that can be running at once. Note that this will be applied
    individually to each instance of auto_submit and so will effectively be doubled
    :param n_max_retries: The maximum number of times a task can be run before being written off as needing user input
    :param tasks_to_run: The tasks to be run for all realisations
    :param sleep_time: The amount of time to sleep between iterations of the auto_submit script
    :param tasks_to_run_with_pattern: A list of (pattern, task_list) pairs to be run. task_list must have dependencies
    already added.
    :param wrapper_logger: The logger to use for wrapper messages
    """

    wrapper_logger.info("Loading estimation models")
    lf_est_model = est.load_full_model(
        join(platform_config[const.PLATFORM_CONFIG.ESTIMATION_MODELS_DIR.name], "LF"), logger=wrapper_logger
    )
    hf_est_model = est.load_full_model(
        join(platform_config[const.PLATFORM_CONFIG.ESTIMATION_MODELS_DIR.name], "HF"), logger=wrapper_logger
    )
    bb_est_model = est.load_full_model(
        join(platform_config[const.PLATFORM_CONFIG.ESTIMATION_MODELS_DIR.name], "BB"), logger=wrapper_logger
    )
    im_est_model = est.load_full_model(
        join(platform_config[const.PLATFORM_CONFIG.ESTIMATION_MODELS_DIR.name], "IM"), logger=wrapper_logger
    )

    bulk_logger = qclogging.get_logger(name="auto_submit_main", threaded=True)
    if debug:
        qclogging.set_stdout_level(bulk_logger, DEBUG)
    qclogging.add_general_file_handler(
        bulk_logger,
        join(
            log_directory,
            MASTER_AUTO_SUBMIT_LOG_FILE_NAME.format(
                datetime.now().strftime(const.TIMESTAMP_FORMAT)
            ),
        ),
    )
    wrapper_logger.debug("Created logger for the main auto_submit thread")

    queue_logger = qclogging.get_logger(name="queue_monitor", threaded=True)
    if debug:
        qclogging.set_stdout_level(queue_logger, DEBUG)
    qclogging.add_general_file_handler(
        queue_logger,
        join(
            log_directory,
            QUEUE_MONITOR_LOG_FILE_NAME.format(
                datetime.now().strftime(const.TIMESTAMP_FORMAT)
            ),
        ),
    )
    wrapper_logger.debug("Created logger for the queue_monitor thread")

    tasks_to_run_with_pattern_and_logger = [
        (
            pattern,
            tasks,
            qclogging.get_logger(name="pattern_{}".format(pattern), threaded=True),
        )
        for pattern, tasks in tasks_to_run_with_pattern
    ]
    for pattern, tasks, logger in tasks_to_run_with_pattern_and_logger:
        qclogging.add_general_file_handler(
            logger,
            join(
                log_directory,
                PATTERN_AUTO_SUBMIT_LOG_FILE_NAME.format(
                    pattern, datetime.now().strftime(const.TIMESTAMP_FORMAT)
                ),
            ),
        )
        wrapper_logger.debug(
            "Created logger for auto_submit with pattern {} and added to list to run".format(
                pattern
            )
        )

    queue_monitor_thread = threading.Thread(
        name="queue monitor",
        daemon=True,
        target=queue_monitor.queue_monitor_loop,
        args=(root_folder, sleep_time, n_max_retries, queue_logger, alert_url),
    )
    wrapper_logger.info("Created queue_monitor thread")

    bulk_auto_submit_thread = threading.Thread(
        name="main auto submit",
        daemon=True,
        target=run_main_submit_loop,
        args=(
            root_folder,
            n_runs,
            "%",
            tasks_to_run,
            sleep_time,
            (lf_est_model, hf_est_model, bb_est_model, im_est_model),
        ),
        kwargs={
            "main_logger": bulk_logger,
            "cycle_timeout": 2 * len(tasks_to_run_with_pattern_and_logger) + 1,
        },
    )
    wrapper_logger.info("Created main auto_submit thread")

    bulk_auto_submit_thread.start()
    if bulk_auto_submit_thread.is_alive():
        wrapper_logger.info("Started main auto_submit thread")
    else:
        thread_not_running = "The queue monitor thread has failed to start"
        wrapper_logger.log(qclogging.NOPRINTCRITICAL, thread_not_running)
        raise RuntimeError(thread_not_running)

    queue_monitor_thread.start()
    if queue_monitor_thread.is_alive():
        wrapper_logger.info("Started queue_monitor thread")
    else:
        thread_not_running = "The main auto_submit thread has failed to start"
        wrapper_logger.log(qclogging.NOPRINTCRITICAL, thread_not_running)
        raise RuntimeError(thread_not_running)
    run_sub_threads = len(tasks_to_run_with_pattern_and_logger) > 0
    while bulk_auto_submit_thread.is_alive() and run_sub_threads:
        wrapper_logger.info("Checking all patterns for tasks to be run")
        for pattern, tasks, pattern_logger in tasks_to_run_with_pattern_and_logger:
            wrapper_logger.info(
                "Loaded pattern {}. Checking for tasks to be run of types: {}".format(
                    pattern, tasks
                )
            )
            run_main_submit_loop(
                root_folder,
                n_runs,
                pattern,
                tasks,
                sleep_time,
                (lf_est_model, hf_est_model, bb_est_model, im_est_model),
                main_logger=pattern_logger,
                cycle_timeout=0,
            )
    bulk_auto_submit_thread.join()
    wrapper_logger.info(
        "The main auto_submit thread has terminated, and all auto_submit patterns have completed a final run through"
    )
    wrapper_logger.info("Attempting to shut down the queue monitor thread")
    queue_monitor.keepAlive = False
    queue_monitor_thread.join(2.0 * sleep_time)
    if not queue_monitor_thread.is_alive():
        wrapper_logger.info("The queue monitor has been shut down successfully")
    else:
        wrapper_logger.critical("The queue monitor has not successfully terminated")


def parse_config_file(
    config_file_location: str, logger: Logger = qclogging.get_basic_logger()
):
    """Takes in the location of a wrapper config file and creates the tasks to be run.
    Requires that the file contains the keys 'run_all_tasks' and 'run_some', even if they are empty
    If the dependencies for a run_some task overlap with those in the tasks_to_run_for_all, as a race condition is
    possible if multiple auto_submit scripts have the same tasks. If multiple run_some instances have the same
    dependencies then this is not an issue as they run sequentially, rather than simultaneously
    :param config_file_location: The location of the config file
    :return: A tuple containing the tasks to be run on all processes and a list of pattern, tasks tuples which state
    which tasks can be run with which patterns
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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "root_folder", help="The root directory of the simulation folder structure"
    )
    parser.add_argument("user", help="Your username")
    parser.add_argument(
        "config_file",
        help="The location of the config file containing everything to be run",
        nargs="?",
        default=join(platform_config[const.PLATFORM_CONFIG.TEMPLATES_DIR.name], "task_config.yaml"),
    )
    parser.add_argument(
        "--sleep_time",
        type=int,
        help="Seconds sleeping between checking queue and adding more jobs",
        default=5,
    )
    parser.add_argument(
        "--n_max_retries",
        help="The maximum number of retries for any given task",
        default=2,
        type=int,
    )
    parser.add_argument(
        "--n_runs",
        default=None,
        type=int,
        nargs="+",
        help="The number of processes each machine can run at once. If a single value is given this is used for all "
        "machines, otherwise one value per machine must be given. The current order is: {}".format(
            list(x.value for x in HPC)
        ),
    )
    parser.add_argument(
        "--log_folder",
        type=str,
        default=".",
        help="Location of the directory to place logs in. Defaults to the value of the root_folder argument. "
        "Must be absolute or relative to the root_folder.",
    )
    parser.add_argument(
        "--debug", action="store_true", help="Print debug messages to stdout"
    )
    parser.add_argument(
        "--alert_url", help="the url to slack alert channel", default=None
    )
    args = parser.parse_args()

    wrapper_logger = qclogging.get_logger(name="cybershake_wrapper", threaded=True)
    master_logger = qclogging.get_logger(name=None, threaded=True, stdout_printer=False)

    if args.debug:
        qclogging.set_stdout_level(wrapper_logger, DEBUG)

    root_directory = abspath(args.root_folder)
    log_directory = join(root_directory, args.log_folder)
    wrapper_log_file = join(
        log_directory,
        WRAPPER_LOG_FILE_NAME.format(datetime.now().strftime(const.TIMESTAMP_FORMAT)),
    )

    master_log_file = join(
        log_directory,
        MASTER_LOG_NAME.format(datetime.now().strftime(const.TIMESTAMP_FORMAT)),
    )
    scheduler_log_file = join(
        log_directory,
        SCHEDULER_LOG_NAME.format(datetime.now().strftime(const.TIMESTAMP_FORMAT)),
    )

    qclogging.add_general_file_handler(master_logger, master_log_file)
    qclogging.add_general_file_handler(wrapper_logger, wrapper_log_file)
    wrapper_logger.info("Logger file added")

    scheduler_logger = qclogging.get_logger(name="scheduler", threaded=True)
    qclogging.add_general_file_handler(scheduler_logger, scheduler_log_file)
    initialise_scheduler(user=args.user, logger=scheduler_logger)

    n_runs = 0
    if args.n_runs is not None:
        if len(args.n_runs) == 1:
            n_runs = {hpc: args.n_runs[0] for hpc in HPC}
            wrapper_logger.debug(
                "Using {} as the maximum number of jobs per machine".format(
                    args.n_runs[0]
                )
            )
        elif len(args.n_runs) == len(HPC):
            n_runs = {}
            for index, hpc in enumerate(HPC):
                wrapper_logger.debug(
                    "Setting {} to have at most {} concurrently running jobs".format(
                        hpc, args.n_runs[index]
                    )
                )
                n_runs.update({hpc: args.n_runs[index]})
        else:
            incorrect_n_runs = (
                "You must specify wither one common value for --n_runs, or one "
                "for each in the following list: {}".format(list(HPC))
            )
            wrapper_logger.log(qclogging.NOPRINTCRITICAL, incorrect_n_runs)
            parser.error(incorrect_n_runs)
    else:
        n_runs = platform_config[const.PLATFORM_CONFIG.DEFAULT_N_RUNS.name]
    wrapper_logger.debug(
        "Machines will allow up to {} jobs to run simultaneously".format(n_runs)
    )

    tasks_n, tasks_to_match = parse_config_file(args.config_file, wrapper_logger)

    run_automated_workflow(
        root_directory,
        log_directory,
        n_runs,
        args.n_max_retries,
        tasks_n,
        args.sleep_time,
        tasks_to_match,
        wrapper_logger,
        args.debug,
        alert_url=args.alert_url,
    )


if __name__ == "__main__":
    main()
