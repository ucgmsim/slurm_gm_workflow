#!/usr/bin/env python3
"""Prints out the current cybershake status.

To reduce re-collection of the same metadata over and over again a is used.
If no file is specified then one is created.
"""
import json
import os
import argparse

import qcore.simulation_structure as sim_struct
from estimation.estimate_cybershake import main as est_cybershake


def main(args: argparse.Namespace):
    root_dir = args.cybershake_root
    runs_dir = sim_struct.get_runs_dir(root_dir)

    if not os.path.isdir(root_dir) or not os.path.isdir(runs_dir):
        print("Not a valid cybershake root directory. Quitting!")
        return

    # Check if temporary json file exists
    if args.temp_file is not None and os.path.isfile(args.temp_file):
        with open(args.temp_dict) as f:
            temp_data = json.load(f)
    else:
        # Run the estimation
        est_args = argparse.Namespace(vms_dir=sim_struct.get_VMs_dir(root_dir),
                                      sources_dir=sim_struct.get_sources_dir(root_dir),
                                      runs_dir=runs_dir,
                                      fault_selection=None,
                                      cybershake_config=None,
                                      output=None,
                                      verbose=False)
        df = est_cybershake(est_args)



    exit()







if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument("cybershake_root", type=str, help="The cybershake root directory")
    parser.add_argument("--temp_file", type=str,
                        help="The temporary json file for repetitive call to this script")

    args = parser.parse_args()

    main(args)