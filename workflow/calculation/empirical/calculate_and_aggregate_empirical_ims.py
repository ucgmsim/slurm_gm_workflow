"""
Uses the median srf, info and rrup files to calculate empirical intensity measures for all events or faults in a
cybershake directory, and then creates all permutations of intensity measure groupings for each event or fault
"""

import argparse
import pathlib

from qcore.qclogging import get_logger, add_general_file_handler

from agg_sim_emp_im_perms import (
    aggregate_simulation_empirical_im_permutations,
)
from calculate_unperturbated_empirical_ims import (
    calculate_unperturbated_empiricals,
)
from empirical.util import classdef


def load_args():
    def absolute_path(path):
        """Takes a path string and returns the absolute path object to it"""
        return pathlib.Path(path).resolve()

    parser = argparse.ArgumentParser(
        """Calculates and aggregates empirical IMs for a simulation directory"""
    )
    parser.add_argument(
        "fault_selection_file",
        type=absolute_path,
        help="Path to the fault selection file containing the list of events to operate on",
    )
    parser.add_argument(
        "simulation_root",
        type=absolute_path,
        help="The directory containing a simulations Data and Runs folders",
        default=pathlib.Path.cwd().resolve(),
        nargs="?",
    )
    parser.add_argument("--version", "-v", help="The version of the simulation")
    parser.add_argument(
        "--n_processes", "-n", help="number of processes", type=int, default=1
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

    args = parser.parse_args()

    errors = []

    if not args.fault_selection_file.is_file():
        errors.append(f"The given file {args.fault_selection_file} doe not exist")

    if errors:
        parser.error("\n".join(errors))

    return args


def main():
    aggregation_logger = get_logger("Empirical_IM_calculator_and_aggregator")
    args = load_args()
    add_general_file_handler(
        aggregation_logger, args.simulation_root / "Empirical_im_calc_and_agg_log.txt"
    )

    calculate_unperturbated_empiricals(
        args.vs30_default,
        args.extended_period,
        args.fault_selection_file,
        args.config,
        args.n_processes,
        args.simulation_root,
        aggregation_logger,
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
