import threading
import argparse
from typing import Dict, List

from qcore import constants as const

from scripts.cybershake.auto_submit import main as run_auto_submit
from scripts.cybershake import queue_monitor
from shared_workflow import workflow_logger


def run_automated_workflow(
    root_folder: str,
    user: str,
    n_runs: Dict[str: int],
    n_max_retries: int,
    rels_to_run: str,
    tasks_to_run: List[str],
    sleep_time: int,
    rels_to_run_once: str,
    tasks_to_run_once: List[str],
):

    bulk_logger = workflow_logger.get_basic_logger()
    queue_logger = workflow_logger.get_basic_logger()
    one_off_logger = workflow_logger.get_basic_logger()

    bulk_auto_submit_thread = threading.Thread(
        target=run_auto_submit,
        args=(
            root_folder,
            user,
            n_runs,
            n_max_retries,
            rels_to_run,
            tasks_to_run,
            sleep_time,
            bulk_logger,
        ),
    )
    queue_monitor_thread = threading.Thread(
        target=queue_monitor.main, args=(root_folder, sleep_time, queue_logger)
    )

    bulk_auto_submit_thread.run()
    queue_monitor_thread.run()
    while bulk_auto_submit_thread.is_alive():
        run_auto_submit(
            root_folder,
            user,
            n_runs,
            n_max_retries,
            rels_to_run_once,
            tasks_to_run_once,
            sleep_time,
            one_off_logger,
        )
    queue_monitor.keepAlive = False


def parse_config_file(config_file_location: str):

    return ['a', 'b'], 'c', ['e', 'f'], 'g'


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "root_folder", help="The root directory of the simulation folder structure"
    )
    parser.add_argument("user", help="Your username")
    parser.add_argument(
        "config_file",
        help="The location of the config file containing everything to be run",
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
        type=list,
        nargs="+",
        help="The number of processes each machine can run at once. If a single value is given this is used for all "
             "machines, otherwise one value per machine must be given. The current order is: {}".format(
            (x.str_value for x in const.HPC)
        ),
    )
    args = parser.parse_args()

    logger = workflow_logger.get_basic_logger()

    n_runs = 0
    if args.n_runs is not None:
        if len(args.n_runs) == 1:
            n_runs = {hpc: args.n_runs[0] for hpc in const.HPC}
            logger.debug(
                "Using {} as the maximum number of jobs per machine".format(n_runs[0])
            )
        elif len(args.n_runs) == len(const.HPC):
            n_runs = {}
            for index, hpc in enumerate(const.HPC):
                logger.debug(
                    "Setting {} to have at most {} concurrently running jobs".format(
                        hpc, args.n_runs[index]
                    )
                )
                n_runs.update({hpc: args.n_runs[index]})
        else:
            logger.critical(
                "Expected either 1 or {} values for --n_runs, got {} values. Specifically: {}. Exiting now".format(
                    len(const.HPC), len(args.n_runs), args.n_runs
                )
            )
            parser.error(
                "You must specify wither one common value for --n_runs, or one "
                "for each in the following list: {}".format(list(const.HPC))
            )
    else:
        n_runs = {const.HPC.maui: 12, const.HPC.mahuika: 12}

    tasks_n, rels_n, tasks_1, rels_1 = parse_config_file(args.config_file)

    run_automated_workflow(
        args.root_folder,
        args.user,
        args.n_runs,
        args.n_max_retries,
        rels_n,
        tasks_n,
        args.sleep_time,
        rels_1,
        tasks_1,
    )


if __name__ == "__main__":
    main()
