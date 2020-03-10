"""
@author: jam335 - jason.motha@canterbury.ac.nz

A script that creates a database and populates it with the status of 
each stage of the run
"""
import argparse
import os

from scripts.management.MgmtDB import MgmtDB


def create_mgmt_db(realisations, db_file, srf_files=[]):
    mgmt_db = MgmtDB.init_db(
        db_file,
        os.path.join(os.path.dirname(os.path.realpath(__file__)), "slurm_mgmt.db.sql"),
    )
    mgmt_db.populate(realisations, srf_files)

    return mgmt_db


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "mgmt_db_file", type=str, help="Path to the management db file to create."
    )
    parser.add_argument(
        "realisations", type=str, nargs="+", help="space delimited list of realisations"
    )
    args = parser.parse_args()

    create_mgmt_db(args.realisations, args.mgmt_db_file)


if __name__ == "__main__":
    main()
