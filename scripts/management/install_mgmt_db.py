"""
A script to create the cybershake management database from a fault selection file.

"""

import argparse
import os
from datetime import datetime
from logging import Logger
from os import path
from typing import List, Tuple

from qcore import simulation_structure
from qcore.constants import TIMESTAMP_FORMAT
from qcore.utils import setup_dir

from scripts.management.MgmtDB import MgmtDB
from shared_workflow import workflow_logger
from shared_workflow.shared_automated_workflow import parse_fsf

INSTALL_DB_LOG_FILE_NAME = "install_database_log_{}.txt"


def create_mgmt_db(
    realisations: List[str],
    root_folder: str,
    srf_files: List[str] = (),
    logger: Logger = workflow_logger.get_basic_logger(),
):
    """
    Creates the management database, adds all tasks for all given realisations and creates the mgmgt db queue
    :param realisations: Realisations to be added to the database
    :param root_folder: The folder the database file is to be put into
    :param srf_files: A list of srf files that the list of realisations can be determined from
    :param logger: The logger of the caller if one is used
    :return: The object representing the new database
    """
    db_file = simulation_structure.get_mgmt_db(root_folder)

    logger.info("Creating database file here: {}".format(db_file))
    mgmt_db = MgmtDB.init_db(
        db_file,
        os.path.join(os.path.dirname(os.path.realpath(__file__)), "slurm_mgmt.db.sql"),
    )

    logger.info("Populating database with {} realisations.".format(len(realisations)))
    mgmt_db.populate(realisations, srf_files)

    mgmt_db_queue = simulation_structure.get_mgmt_db_queue(root_folder)
    logger.info("Creating mgmt db queue folder {}".format(mgmt_db_queue))
    setup_dir(mgmt_db_queue)

    return mgmt_db


def create_mgmt_db_from_faults(
    fault_count_pairs: List[Tuple[str, int]],
    root_folder: str,
    logger: Logger = workflow_logger.get_basic_logger(),
):
    """

    :param fault_count_pairs: A list of ()
    :param root_folder:
    :param logger:
    :return:
    """
    realisations = []
    for fault, rel_count in fault_count_pairs:
        logger.info("Adding {} realisations for the fault {}.".format(rel_count, fault))
        realisations.extend(
            [
                simulation_structure.get_realisation_name(fault, i)
                for i in range(1, rel_count + 1)
            ]
        )
    return create_mgmt_db(realisations, root_folder, logger=logger)


def main():

    logger = workflow_logger.get_logger("install_mgmt_db")

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "path_cybershake",
        type=str,
        help="the path to the root of a specific version cybershake.",
    )
    parser.add_argument(
        "fault_selection_file", type=str, help="The fault selection file"
    )
    args = parser.parse_args()

    root_folder = path.abspath(args.path_cybershake)

    workflow_logger.add_general_file_handler(
        logger,
        path.join(
            root_folder,
            INSTALL_DB_LOG_FILE_NAME.format(datetime.now().strftime(TIMESTAMP_FORMAT)),
        ),
    )

    faults = parse_fsf(os.path.abspath(args.fault_selection_file))

    create_mgmt_db_from_faults(faults, root_folder, logger=logger)


if __name__ == "__main__":
    main()
