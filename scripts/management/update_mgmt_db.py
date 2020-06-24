"""
@author: jam335 - jason.motha@canterbury.ac.nz
A script that updates a slurm mgmt db and updates the status of a task
"""

import argparse
import os

from scripts.management import db_helper
from scripts.management.MgmtDB import MgmtDB, SchedulerTask
import qcore.constants as const


def update_db(root_folder, process, status, run_name, job_id, error):
    """Update the database with the given values"""

    entry = SchedulerTask(run_name, process, status, job_id, error)
    database = MgmtDB(root_folder)

    # If we are running this manually then we should set the retry limit to be above a reasonable value for manual
    # submissions
    database.update_entries_live([entry], 256)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "run_folder", type=str, help="folder to the collection of runs on the HPC"
    )
    parser.add_argument("run_name", type=str, help="name of run to be updated")
    parser.add_argument("process", choices=db_helper.enum_to_list(const.ProcessType))
    parser.add_argument(
        "status", type=str, choices=db_helper.enum_to_list(const.Status)
    )
    parser.add_argument("-j", "--job", type=int, default=None)
    parser.add_argument("-e", "--error", type=str, default=None)

    args = parser.parse_args()

    update_db(
        os.path.abspath(args.run_folder),
        args.process,
        args.status,
        args.run_name,
        args.job,
        args.error,
    )
