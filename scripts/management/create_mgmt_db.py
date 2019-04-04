"""
@author: jam335 - jason.motha@canterbury.ac.nz

A script that creates a database and populates it with the status of 
each stage of the run
"""
import argparse
import os

from scripts.management.MgmtDB import MgmtDB

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "mgmt_db_file", type=str, help="Path to the management db file to create."
    )
    parser.add_argument(
        "realisations", type=str, nargs="+", help="space delimited list of realisations"
    )
    args = parser.parse_args()

    mgmt_db = MgmtDB.init_db(
        args.mgmt_db_file,
        os.path.join(os.path.dirname(os.path.realpath(__file__)), "slurm_mgmt.db.sql"),
    )
    mgmt_db.populate(args.realisations)


if __name__ == "__main__":
    main()
