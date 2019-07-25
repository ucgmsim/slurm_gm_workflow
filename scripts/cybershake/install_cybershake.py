import argparse
from datetime import datetime
import os

from qcore.constants import TIMESTAMP_FORMAT
from qcore.qclogging import add_general_file_handler, get_logger

from scripts.cybershake.install_cybershake_fault import install_fault
from shared_workflow import workflow_logger
from shared_workflow.shared_defaults import recipe_dir

from qcore.constants import ROOT_DEFAULTS_FILE_NAME, HF_DEFAULT_SEED

AUTO_SUBMIT_LOG_FILE_NAME = "install_cybershake_log_{}.txt"


def main():
    logger = get_logger()

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "path_cybershake",
        type=str,
        help="the path to the root of a specific version cybershake.",
    )
    parser.add_argument(
        "fault_selection_list", type=str, help="The fault selection file"
    )
    parser.add_argument(
        "version",
        type=str,
        default="16.1",
        help="Please specify GMSim version",
        nargs="?",
    )
    parser.add_argument(
        "--seed",
        type=str,
        default=HF_DEFAULT_SEED,
        help="The seed to be used for HF simulations. Default is to request a random seed.",
    )
    parser.add_argument(
        "--stat_file_path",
        type=str,
        default="/nesi/project/nesi00213/StationInfo/non_uniform_whole_nz_with_real_stations-hh400_v18p6p2.ll",
        help="The path to the station info file path.",
    )
    parser.add_argument(
        "--extended_period",
        action="store_true",
        help="Should IM_calc calculate more psa periods.",
    )
    parser.add_argument(
        "--log_file",
        type=str,
        default=None,
        help="Location of the log file to use. Defaults to 'cybershake_log.txt' in the location root_folder. "
        "Must be absolute or relative to the root_folder.",
    )

    args = parser.parse_args()

    path_cybershake = os.path.abspath(args.path_cybershake)

    if args.log_file is None:
        add_general_file_handler(
            logger,
            os.path.join(
                path_cybershake,
                AUTO_SUBMIT_LOG_FILE_NAME.format(
                    datetime.now().strftime(TIMESTAMP_FORMAT)
                ),
            ),
        )
    else:
        add_general_file_handler(
            logger, os.path.join(path_cybershake, args.log_file)
        )
    logger.debug("Added file handler to the logger")

    if not os.path.exists(
        os.path.join(recipe_dir, "gmsim", args.version)
    ) or os.path.isfile(os.path.join(recipe_dir, "gmsim", args.version)):
        logger.critical(
            "Version {} does not exist in templates/gmsim directory.".format(
                args.version
            )
        )
        parser.error(
            "Version {} does not exist, place a directory with that name into {}\n"
            "Also ensure it has contents of {} and {}".format(
                args.version,
                os.path.join(recipe_dir, "gmsim"),
                ROOT_DEFAULTS_FILE_NAME,
                "emod3d_defaults.yaml",
            )
        )
    for f_name in [ROOT_DEFAULTS_FILE_NAME, "emod3d_defaults.yaml"]:
        if not os.path.exists(os.path.join(recipe_dir, "gmsim", args.version, f_name)):
            logger.critical(
                "Version {} does not have the file {}".format(args.version, f_name)
            )
            parser.error(
                "Version {} does not have a required {} file in the directory {}".format(
                    args.version,
                    f_name,
                    os.path.join(recipe_dir, "gmsim", args.version),
                )
            )

    faults = {}
    with open(args.fault_selection_list) as fault_file:
        for line in fault_file.readlines():
            fault, count, *_ = line.split()
            count = int(count[:-1])
            faults.update({fault: count})

    for fault, count in faults.items():
        install_fault(
            fault,
            count,
            path_cybershake,
            args.version,
            args.stat_file_path,
            args.seed,
            args.extended_period,
            workflow_logger.get_realisation_logger(logger, fault),
        )


if __name__ == "__main__":
    main()
