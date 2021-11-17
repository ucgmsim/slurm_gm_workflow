import argparse
from datetime import datetime
from os import path

from qcore.constants import (
    TIMESTAMP_FORMAT,
    ROOT_DEFAULTS_FILE_NAME,
    PLATFORM_CONFIG,
    HF_DEFAULT_SEED,
    Components,
)
from qcore import qclogging

from install_cybershake_fault import install_fault
from workflow.automation.platform_config import platform_config

AUTO_SUBMIT_LOG_FILE_NAME = "install_cybershake_log_{}.txt"


def load_args(logger):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "path_cybershake",
        type=path.abspath,
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
        "--components",
        choices=list(Components.iterate_str_values()),
        nargs="+",
        default=None,
        help="list of components to run IM_calcs, overwrites value in selected gmsim version template",
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
        "--no_check_vm",
        action="store_false",
        dest="check_vm",
        help="Set this flag if you are generating VMs from the automated workflow",
        default=True,
    )

    vm_pert = parser.add_mutually_exclusive_group()
    vm_pert.add_argument(
        "--vm_perturbations",
        action="store_true",
        help="Use velocity model perturbations. If this is selected all realisations must have a perturbation file.",
    )
    vm_pert.add_argument(
        "--ignore_vm_perturbations",
        action="store_true",
        help="Don't use velocity model perturbations. If this is selected any perturbation files will be ignored.",
    )

    qp_qs = parser.add_mutually_exclusive_group()
    qp_qs.add_argument(
        "--vm_qpqs_files",
        action="store_true",
        help="Use generated Qp/Qs files. If this is selected all events/faults must have Qp and Qs files.",
    )
    qp_qs.add_argument(
        "--ignore_vm_qpqs_files",
        action="store_true",
        help="Don't use generated Qp/Qs files. If this is selected any Qp/Qs files will be ignored.",
    )
    args = parser.parse_args()

    if args.log_file is None:
        qclogging.add_general_file_handler(
            logger,
            path.join(
                args.path_cybershake,
                AUTO_SUBMIT_LOG_FILE_NAME.format(
                    datetime.now().strftime(TIMESTAMP_FORMAT)
                ),
            ),
        )
    else:
        qclogging.add_general_file_handler(
            logger, path.join(args.path_cybershake, args.log_file)
        )
    logger.debug("Added file handler to the logger")
    logger.debug(f"Arguments are as follows: {args}")

    messages = []

    gmsim_version_path = path.join(
        platform_config[PLATFORM_CONFIG.TEMPLATES_DIR.name], "gmsim", args.version
    )

    if not path.exists(gmsim_version_path) or path.isfile(gmsim_version_path):
        messages.append(
            "Version {} does not exist, place a directory with that name into {}\n"
            "Also ensure it has contents of {} and {}".format(
                args.version,
                path.join(platform_config[PLATFORM_CONFIG.TEMPLATES_DIR.name], "gmsim"),
                ROOT_DEFAULTS_FILE_NAME,
                "emod3d_defaults.yaml",
            )
        )
    else:
        for f_name in [ROOT_DEFAULTS_FILE_NAME, "emod3d_defaults.yaml"]:
            if not path.exists(path.join(gmsim_version_path, f_name)):
                messages.append(
                    "Version {} does not have a required {} file in the directory {}".format(
                        args.version,
                        f_name,
                        path.join(
                            platform_config[PLATFORM_CONFIG.TEMPLATES_DIR.name],
                            "gmsim",
                            args.version,
                        ),
                    )
                )

    if len(messages) > 0:
        message = "\n".join(messages)
        logger.error(message)
        parser.error(message)

    return args


def main():
    logger = qclogging.get_logger()
    args = load_args(logger)

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
            args.path_cybershake,
            args.version,
            args.stat_file_path,
            args.seed,
            args.extended_period,
            vm_perturbations=args.vm_perturbations,
            ignore_vm_perturbations=args.ignore_vm_perturbations,
            vm_qpqs_files=args.vm_qpqs_files,
            ignore_vm_qpqs_files=args.ignore_vm_qpqs_files,
            keep_dup_station=args.keep_dup_station,
            components=args.components,
            logger=qclogging.get_realisation_logger(logger, fault),
            check_vm=args.check_vm,
        )


if __name__ == "__main__":
    main()
