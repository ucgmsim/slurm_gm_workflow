"""
Creates all aggregation empirical intensity measure files for all events or faults in a simulation structure directory
from the available intensity measure files in the verification folder of the first realisation of each fault or event
"""
import argparse
import pathlib
from logging import Logger
from multiprocessing.pool import Pool

from qcore.formats import load_fault_selection_file
from qcore.qclogging import (
    get_logger,
    get_basic_logger,
    get_realisation_logger,
    add_general_file_handler,
)
from qcore.simulation_structure import get_realisation_name, get_empirical_dir

from empirical.scripts.aggregate_empirical_im_permutations import agg_emp_perms


def load_args():
    def absolute_path(path):
        """Takes a path string and returns the absolute path object to it"""
        return pathlib.Path(path).resolve()

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "fault_selection_file",
        type=absolute_path,
        help="Path to the fault selection file containing the list of events to operate on",
    )
    parser.add_argument(
        "simulation_root",
        type=absolute_path,
        help="The directory containing a simulations Data and Runs folders",
        default=pathlib.Path("").resolve(),
        nargs="?",
    )
    parser.add_argument("--version", "-v", help="The version of the simulation")
    parser.add_argument(
        "--n_processes", "-n", help="number of processes", type=int, default=1
    )

    args = parser.parse_args()

    errors = []

    if not args.fault_selection_file.is_file():
        errors.append(f"The given file {args.fault_selection_file} doe not exist")

    return args


def aggregate_simulation_empirical_im_permutations(
    fsf, n_processes, sim_root, version, logger: Logger = get_basic_logger()
):
    events = load_fault_selection_file(fsf)
    logger.debug(f"Loaded {len(events)} events from the fault selection file")
    events = [
        name if count == 1 else get_realisation_name(name, 1)
        for name, count in events.items()
    ]
    worker_pool = Pool(n_processes)
    worker_pool.starmap(
        agg_emp_perms,
        [
            (
                pathlib.Path(get_empirical_dir(sim_root, event)),
                event,
                version,
                get_realisation_logger(logger, event).name,
            )
            for event in events
        ],
    )


def main():
    aggregation_logger = get_logger("Aggregate_simulation_empirical_im_permutations")
    args = load_args()
    add_general_file_handler(
        aggregation_logger, args.simulation_root / "add_sim_emp_im_perms_log.txt"
    )
    aggregate_simulation_empirical_im_permutations(
        args.fault_selection_file,
        args.n_processes,
        args.simulation_root,
        args.version,
        aggregation_logger,
    )


if __name__ == "__main__":
    main()
