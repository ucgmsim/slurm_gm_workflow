"""
@author: jam335 - jason.motha@canterbury.ac.nz

A script that creates a database and populates it with the status of 
each stage of the run
"""
import argparse
import os

from scripts.management.install_mgmt_db import create_mgmt_db


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "cybershake_directory", type=str, help="Path to the cybershake root directory."
    )
    parser.add_argument(
        "realisations", type=str, nargs="+", help="space delimited list of realisations"
    )
    args = parser.parse_args()

    root_folder = os.path.abspath(args.cybershake_directory)
    create_mgmt_db(args.realisations, root_folder)


if __name__ == "__main__":
    main()
