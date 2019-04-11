"""
@author: jam335 - jason.motha@canterbury.ac.nz

A script that updates a slurm mgmt db and inserts a new task to run
"""
import argparse
from scripts.management.MgmtDB import MgmtDB
import qcore.constants as const


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "mgmt_db_file", type=str, help="Path to the management db to use"
    )
    parser.add_argument('run_name', type=str,
                        help='name of run to be updated')
    parser.add_argument('process', choices=[proc_type.str_value for proc_type in const.ProcessType])
    args = parser.parse_args()

    mgmt_db = MgmtDB(args.mgmt_db_file)
    mgmt_db.insert(args.run_name, const.ProcessType.from_str(args.process).value)

if __name__ == '__main__':
    main()
