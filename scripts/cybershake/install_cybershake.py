import argparse
from datetime import datetime
import os

from qcore.constants import TIMESTAMP_FORMAT

from scripts.cybershake.install_cybershake_fault import install_fault
from shared_workflow import workflow_logger

AUTO_SUBMIT_LOG_FILE_NAME = "install_cybershake_log_{}.txt"


def main():
    logger = workflow_logger.get_logger()

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "path_cybershake",
        type=str,
        help="the path to the root of a specific version cybershake.",
    )
    parser.add_argument(
        "version", type=str, default="16.1", help="Please specify GMSim version"
    )
    parser.add_argument(
        "fault_selection_list", type=str, help="The fault selection file"
    )
    parser.add_argument(
        "--seed",
        type=str,
        default=0,
        help="The seed to be used for HF simulations. Default is to request a random seed.",
    )
    parser.add_argument(
        "--log_file",
        type=str,
        default=None,
        help="Location of the log file to use. Defaults to 'cybershake_log.txt' in the location root_folder. "
        "Must be absolute or relative to the root_folder.",
    )

    args = parser.parse_args()

    if args.log_file is None:
        workflow_logger.add_general_file_handler(
            logger,
            os.path.join(
                args.root_folder,
                AUTO_SUBMIT_LOG_FILE_NAME.format(
                    datetime.now().strftime(TIMESTAMP_FORMAT)
                ),
            ),
        )
    else:
        workflow_logger.add_general_file_handler(
            logger, os.path.join(args.root_folder, args.log_file)
        )
    logger.debug("Added file handler to the logger")

    faults = {}
    with open(args.fault_selection_list) as fault_file:
        for line in fault_file.readlines():
            fault, count, *_ = line.split(" ")
            count = int(count[:-2])
            faults.update({fault: count})

    for fault, count in faults.items():
        install_fault(
            fault,
            count,
            args.path_cybershake,
            args.version,
            args.seed,
            workflow_logger.get_realisation_logger(logger, fault),
        )


if __name__ == "__main__":
    main()
