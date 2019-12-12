"""
Uses median info and rrup files to generate empirical intensity measure values for all of the events or faults in a
simulation structure
"""

import argparse
import pathlib
from logging import Logger
from multiprocessing.pool import Pool

from qcore.formats import load_fault_selection_file
from qcore.qclogging import get_basic_logger, add_general_file_handler, get_logger
from qcore.simulation_structure import (
    get_sources_dir,
    get_realisation_name,
    get_fault_from_realisation,
    get_srf_info_location,
    get_rrup_location,
    get_rrup_path,
)
from qcore.utils import load_yaml

from empirical.scripts.calculate_empirical import IM_LIST, calculate_empirical
from empirical.util import classdef


def load_args():
    parser = argparse.ArgumentParser("""Script to generate """)

    def absolute_path(path):
        """Takes a path string and returns the absolute path object to it"""
        return pathlib.Path(path).resolve()

    parser.add_argument(
        "fault_selection_file",
        type=absolute_path,
        help="The path to the file listing the faults or events in a simulation",
    )
    parser.add_argument(
        "simulation_root",
        type=absolute_path,
        help="The directory containing a simulations Data and Runs folders",
        default=pathlib.Path(".").resolve(),
        nargs="?",
    )
    parser.add_argument(
        "--vs30_default",
        default=classdef.VS30_DEFAULT,
        help="Sets the default value for the vs30",
    )
    parser.add_argument(
        "--config",
        "-c",
        help="configuration file to select which model is being used",
        type=absolute_path,
    )
    parser.add_argument(
        "--extended_period",
        "-e",
        action="store_true",
        help="Indicate the use of extended(100) pSA periods",
    )
    parser.add_argument(
        "--n_processes", "-n", help="number of processes", type=int, default=1
    )

    args = parser.parse_args()

    return args


def create_event_tasks(
    events, sim_root, config_file, vs30_default, extended_period, empirical_im_logger
):
    tasks = []
    sources = pathlib.Path(get_sources_dir(sim_root))
    vs30_file = load_yaml(pathlib.Path(sim_root) / "Runs" / "root_params.yaml")[
        "stat_vs_est"
    ]
    for realisation_name in events:
        empirical_im_logger.debug(
            f"Generating calculation tasks for {realisation_name}"
        )
        event_name = get_fault_from_realisation(realisation_name)
        fault_info = sources / get_srf_info_location(event_name)
        output_dir = get_rrup_location(sim_root, realisation_name)
        rupture_distance = get_rrup_path(sim_root, realisation_name)
        tasks.append(
            [
                event_name,
                fault_info,
                output_dir,
                config_file,
                None,
                vs30_file,
                vs30_default,
                IM_LIST,
                rupture_distance,
                200,
                extended_period,
            ]
        )
        empirical_im_logger.debug(f"Added task {tasks[-1]} to the task list")
    return tasks


def main():
    uei_logger = get_logger("Unperturbated_empirical_ims")
    args = load_args()
    add_general_file_handler(
        uei_logger, args.simulation_root / "unperturbated_empirical_ims_log.txt"
    )

    calculate_unperturbated_empiricals(
        args.vs30_default,
        args.extended_period,
        args.fault_selection_file,
        args.config,
        args.n_processes,
        args.simulation_root,
        uei_logger,
    )


def calculate_unperturbated_empiricals(
    default_vs30,
    extended_period,
    fsf,
    im_config,
    n_processes,
    sim_root,
    empirical_im_logger: Logger = get_basic_logger(),
):
    events = load_fault_selection_file(fsf)
    empirical_im_logger.debug(
        f"Loaded {len(events)} events from the fault selection file"
    )
    events = [
        name if count == 1 else get_realisation_name(name, 1)
        for name, count in events.items()
    ]
    tasks = create_event_tasks(
        events, sim_root, im_config, default_vs30, extended_period, empirical_im_logger
    )

    pool = Pool(min(n_processes, len(tasks)))
    empirical_im_logger.debug(f"Running empirical im calculations")
    pool.starmap(calculate_empirical, tasks)
    empirical_im_logger.debug(f"Empirical ims calculated")


if __name__ == "__main__":
    main()
