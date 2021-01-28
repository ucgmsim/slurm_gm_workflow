import argparse
from datetime import datetime
import os

from qcore.constants import (
    TIMESTAMP_FORMAT,
    ROOT_DEFAULTS_FILE_NAME,
    PLATFORM_CONFIG,
    HF_DEFAULT_SEED,
)
from qcore import qclogging

from scripts.cybershake.install_cybershake_fault import install_fault
from shared_workflow.platform_config import platform_config

AUTO_SUBMIT_LOG_FILE_NAME = "install_cybershake_log_{}.txt"


def main():
    logger = qclogging.get_logger()

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
        default="/nesi/project/nesi00213/StationInfo/non_uniform_whole_nz_with_real_stations-hh400_v20p3_land.ll",
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
    parser.add_argument(
        "--keep_dup_station",
        action="store_true",
        help="Keep stations if they snap to the same grid-point",
    )
    parser.add_argument(
        "--vm_perturbations",
        action="store_true",
        help="Use velocity model perturbations. If this is selected all realisations must have a perturbation file.",
    )
    parser.add_argument(
        "--ignore_vm_perturbations",
        action="store_true",
        help="Don't use velocity model perturbations. If this is selected any perturbation files will be ignored.",
    )

    args = parser.parse_args()

    if args.vm_perturbations and args.ignore_vm_perturbations:
        parser.error(
            "Both --vm_perturbations and --ignore_vm_perturbations cannot be set at the same time."
        )

    path_cybershake = os.path.abspath(args.path_cybershake)

    if args.log_file is None:
        qclogging.add_general_file_handler(
            logger,
            os.path.join(
                path_cybershake,
                AUTO_SUBMIT_LOG_FILE_NAME.format(
                    datetime.now().strftime(TIMESTAMP_FORMAT)
                ),
            ),
        )
    else:
        qclogging.add_general_file_handler(
            logger, os.path.join(path_cybershake, args.log_file)
        )
    logger.debug("Added file handler to the logger")
    logger.debug(f"Arguments are as follows: {args}")

    if not os.path.exists(
        os.path.join(
            platform_config[PLATFORM_CONFIG.TEMPLATES_DIR.name], "gmsim", args.version
        )
    ) or os.path.isfile(
        os.path.join(
            platform_config[PLATFORM_CONFIG.TEMPLATES_DIR.name], "gmsim", args.version
        )
    ):
        logger.critical(
            "Version {} does not exist in templates/gmsim directory.".format(
                args.version
            )
        )
        parser.error(
            "Version {} does not exist, place a directory with that name into {}\n"
            "Also ensure it has contents of {} and {}".format(
                args.version,
                os.path.join(
                    platform_config[PLATFORM_CONFIG.TEMPLATES_DIR.name], "gmsim"
                ),
                ROOT_DEFAULTS_FILE_NAME,
                "emod3d_defaults.yaml",
            )
        )
    for f_name in [ROOT_DEFAULTS_FILE_NAME, "emod3d_defaults.yaml"]:
        if not os.path.exists(
            os.path.join(
                platform_config[PLATFORM_CONFIG.TEMPLATES_DIR.name],
                "gmsim",
                args.version,
                f_name,
            )
        ):
            logger.critical(
                "Version {} does not have the file {}".format(args.version, f_name)
            )
            parser.error(
                "Version {} does not have a required {} file in the directory {}".format(
                    args.version,
                    f_name,
                    os.path.join(
                        platform_config[PLATFORM_CONFIG.TEMPLATES_DIR.name],
                        "gmsim",
                        args.version,
                    ),
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
            vm_perturbations=args.vm_perturbations,
            ignore_vm_perturbations=args.ignore_vm_perturbations,
            keep_dup_station=args.keep_dup_station,
            logger=qclogging.get_realisation_logger(logger, fault),
        )


if __name__ == "__main__":
    main()
