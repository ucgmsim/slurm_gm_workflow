#!/usr/bin/env python3
"""Prints out the current cybershake status.

To reduce re-collection of the same metadata over and over again a is used.
If no file is specified then one is created.

Note: Assumes that faults are run in alphabetical order!
"""
import json
import glob
import os
import argparse
from typing import List

import numpy as np
import pandas as pd

import qcore.simulation_structure as sim_struct
import qcore.constants as const
from estimation.estimate_cybershake import main as est_cybershake

# The process types that are used for progress tracking
PROCESS_TYPES = [
    const.ProcessType.EMOD3D.str_value,
    const.ProcessType.HF.str_value,
    const.ProcessType.BB.str_value,
]

EST_CORE_HOURS_COL = "est_core_hours"
ACT_CORE_HOURS_COL = "act_core_hours"

def get_chours(json_file, proc_types: List[str]):
    """Gets the core hours for the specified process types (i.e. EMOD3D, HF, BB, ...)
    for the specified simulation metadata json log file.
    """
    with open(json_file, "r") as f:
        data = json.load(f)

    sim_name = data.get(const.MetadataField.sim_name.value)
    core_hours = []
    for proc_type in proc_types:
        if proc_type in data.keys():
            core_hours.append(data[proc_type].get(const.MetadataField.core_hours.value))
        else:
            core_hours.append(None)

    return sim_name, core_hours


def get_chours_used(fault_dirs: List[str]):
    """Returns a dataframe containing the core hours used for each of the
    specified faults"""
    faults, core_hours = [], []
    for fault in fault_dirs:
        fault_name = os.path.basename(fault)

        cur_fault_chours = [
            get_chours(json_file, PROCESS_TYPES)[1]
            for json_file in glob.glob(
                os.path.join(fault, "**", const.METADATA_LOG_FILENAME), recursive=True
            )
        ]

        if len(core_hours) > 0:
            faults.append(fault_name)
            core_hours.append(sum(cur_fault_chours))

    if len(faults) > 0:
        df = pd.DataFrame(
            index=faults, data=[core_hours], columns=[const.MetadataField.core_hours.value]
        )
        return df
    return None


def get_faults_and_r_count(cybershake_list: str):
    with open(cybershake_list, 'r') as f:
        lines = f.readlines()

    fault_names, r_counts = [], []
    for ix, line in enumerate(lines):
        line_l = line.split(' ')
        fault_names.append(line_l[0].strip())

        try:
            r_counts.append(int(line_l[1].strip().rstrip('r')))
        except ValueError as ex:
            print("Failed to read line {} of cybershake list. Need to know"
                  "the number of realisations for each fault, quitting!".format(ix + 1))
            # Raise the exception so execution stops
            raise

    return np.asarray(fault_names), np.asarray(r_counts)


def main(args: argparse.Namespace):
    root_dir = args.cybershake_root
    runs_dir = sim_struct.get_runs_dir(root_dir)

    cybershake_list = sim_struct.get_cybershake_list(root_dir)
    fault_names, r_counts = get_faults_and_r_count(cybershake_list)

    # Sort, as faults are run in alphabetical order
    sort_ind = np.argsort(fault_names)
    fault_names, r_counts = fault_names[sort_ind], r_counts[sort_ind]

    faults = [sim_struct.get_fault_dir(root_dir, fault_name) for fault_name in fault_names]

    if not os.path.isdir(root_dir) or not os.path.isdir(runs_dir):
        print("Not a valid cybershake root directory. Quitting!")
        return

    # Check if temporary json file exists
    if args.temp_file is not None and os.path.isfile(args.temp_file):
        with open(args.temp_dict) as f:
            temp_data = json.load(f)
    else:
        temp_data = {}

        # Run the estimation
        est_args = argparse.Namespace(
            vms_dir=sim_struct.get_VM_dir(root_dir),
            sources_dir=sim_struct.get_sources_dir(root_dir),
            runs_dir=runs_dir,
            fault_selection=None,
            cybershake_config=None,
            output=None,
            verbose=False,
        )
        df = est_cybershake(est_args)
        grouped_df = df.groupby("fault_name").sum()
        grouped_df.sort_index(axis=0, level=0, inplace=True)

        if grouped_df.index.values == fault_names:
            print("The fault names of the estimation results and directories "
                  "are not matching. These have to match, quitting!")
            exit()

        # Create progress dataframe
        column_t = []
        for proc_type in PROCESS_TYPES:
            column_t.append((proc_type, EST_CORE_HOURS_COL))
            column_t.append((proc_type, ACT_CORE_HOURS_COL))
        column_t.append(("total", EST_CORE_HOURS_COL))
        column_t.append(("total", ACT_CORE_HOURS_COL))

        progress_df = pd.DataFrame(index=grouped_df.index.values,
                                   columns=pd.MultiIndex.from_tuples(column_t))

        # Populate progress dataframe with estimation data
        total = None
        for proc_type in PROCESS_TYPES:
            values = grouped_df[proc_type, const.MetadataField.core_hours.value]
            progress_df[proc_type, EST_CORE_HOURS_COL] = values
            total = values if total is None else total + values
        progress_df["total", EST_CORE_HOURS_COL] = total
        progress_df["completed_realisations"] = 0
        progress_df["n_realisations"] = r_counts


        # Get actual core hours for all faults, assumes faults are run
        # in alphabetical order
        chours_df = get_chours_used(faults)



    exit()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "cybershake_root", type=str, help="The cybershake root directory"
    )
    parser.add_argument(
        "--temp_file",
        type=str,
        help="The temporary file for repetitive call to this script",
    )

    args = parser.parse_args()

    main(args)
