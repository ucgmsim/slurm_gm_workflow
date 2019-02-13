#!/usr/bin/env python3
"""Prints out the current cybershake status.

To reduce re-collection of the same metadata over and over again a csv is created
 which stores the  progress dataframe.

Note: Assumes that faults are run in alphabetical order!

The missing data column of the progress dataframe is set to True if any
metadata for any realisation of that fault.
"""
import json
import glob
import os
import argparse
from typing import List, Iterable

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

COMPLETED_R_COUNT_COL = "completed_r_count"
R_COUNT_COL = "r_count"
MISSING_DATA_FLAG_COL = "missing_data"


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
            core_hours.append(
                data[proc_type].get(const.MetadataField.run_time.value, np.nan)
                * data[proc_type].get(const.MetadataField.n_cores.value, np.nan)
            )
        else:
            core_hours.append(np.nan)

    return sim_name, core_hours


def get_chours_used(fault_dirs: Iterable[str]):
    """Returns a dataframe containing the core hours used for each of the
    specified faults"""
    faults, core_hours, r_counts, missing_data = [], [], [], []
    for fault in fault_dirs:
        fault_name = os.path.basename(fault)

        cur_fault_chours = np.asarray(
            [
                get_chours(json_file, PROCESS_TYPES)[1]
                for json_file in glob.glob(
                    os.path.join(fault, "**", const.METADATA_LOG_FILENAME),
                    recursive=True,
                )
            ]
        ).reshape(-1, len(PROCESS_TYPES))

        if np.any(np.isnan(cur_fault_chours)):
            print(
                "Some metadata for fault {} is missing. If the fault is still"
                "running then this to be expected, otherwise this might "
                "be worth investigating".format(fault)
            )
            missing_data.append(True)
        else:
            missing_data.append(False)

        r_counts.append(cur_fault_chours.shape[0])

        faults.append(fault_name)
        if cur_fault_chours.shape[0] > 0:
            core_hours.append(np.nansum(cur_fault_chours, axis=0))
        else:
            core_hours.append([np.nan, np.nan, np.nan])

    df = pd.DataFrame(index=faults, data=core_hours, columns=PROCESS_TYPES)
    df[COMPLETED_R_COUNT_COL] = r_counts
    df[MISSING_DATA_FLAG_COL] = missing_data

    return df


def get_faults_and_r_count(cybershake_list: str):
    """Gets the fault names and number of realisations from a cybershake fault list."""
    with open(cybershake_list, "r") as f:
        lines = f.readlines()

    fault_names, r_counts = [], []
    for ix, line in enumerate(lines):
        line_l = line.split(" ")
        fault_names.append(line_l[0].strip())

        try:
            r_counts.append(int(line_l[1].strip().rstrip("r")))
        except ValueError as ex:
            print(
                "Failed to read line {} of cybershake list. Need to know"
                "the number of realisations for each fault, quitting!".format(ix + 1)
            )
            # Raise the exception so execution stops
            raise

    return np.asarray(fault_names), np.asarray(r_counts)


def get_new_progress_df(root_dir, runs_dir, faults, fault_names, r_counts):
    """Gets a new progress dataframe, runs the full estimation + collects
    all actual core hours.
    """
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

    # Sanity check
    if np.any(grouped_df.index.values != fault_names):
        raise Exception(
            "The fault names of the estimation results and directories "
            "are not matching. These have to match, quitting!"
        )

    # Create progress dataframe
    column_t = []
    for proc_type in PROCESS_TYPES:
        column_t.append((proc_type, EST_CORE_HOURS_COL))
        column_t.append((proc_type, ACT_CORE_HOURS_COL))
    column_t.append(("total", EST_CORE_HOURS_COL))
    column_t.append(("total", ACT_CORE_HOURS_COL))

    progress_df = pd.DataFrame(
        index=grouped_df.index.values, columns=pd.MultiIndex.from_tuples(column_t)
    )

    # Populate progress dataframe with estimation data
    total = np.nan
    for proc_type in PROCESS_TYPES:
        values = grouped_df[proc_type, const.MetadataField.core_hours.value]
        progress_df[proc_type, EST_CORE_HOURS_COL] = values
        total = values if total is np.nan else total + values
    progress_df["total", EST_CORE_HOURS_COL] = total
    progress_df["total", COMPLETED_R_COUNT_COL] = 0
    progress_df["total", R_COUNT_COL] = r_counts

    # Get actual core hours for all faults, assumes faults are run
    # in alphabetical order
    chours_df = get_chours_used(faults)

    # Add actual data to progress df
    total = np.nan
    for proc_type in PROCESS_TYPES:
        values = chours_df[proc_type]
        progress_df[proc_type, ACT_CORE_HOURS_COL] = values
        total = values if total is np.nan else total + values
    progress_df["total", ACT_CORE_HOURS_COL] = total
    progress_df["total", COMPLETED_R_COUNT_COL] = chours_df[COMPLETED_R_COUNT_COL]
    progress_df["total", MISSING_DATA_FLAG_COL] = chours_df[MISSING_DATA_FLAG_COL]

    return progress_df


def load_progress_df(file: str):
    return pd.read_csv(file, index_col=[0], header=[0, 1])


def print_progress(progress_df: pd.DataFrame, cur_fault_ix: int = None):
    """Prints the progress dataframe in a nice format.

    If cur_fault_ix is specified then all uncompleted faults are printed and
    the latest completed fault with the previous and next five faults.

    Otherwise all faults are printed.
    """

    def get_usage_str(fault: str, proc_type: str):
        return "{:.2f}/{:.2f}".format(
            progress_df.loc[fault, (proc_type, ACT_CORE_HOURS_COL)],
            progress_df.loc[fault, (proc_type, EST_CORE_HOURS_COL)],
        )

    if cur_fault_ix is not None:
        to_print_mask = (
            progress_df["total", COMPLETED_R_COUNT_COL].values
            != progress_df["total", R_COUNT_COL].values
        ) & (progress_df["total", COMPLETED_R_COUNT_COL].values > 0)

        to_print_mask[cur_fault_ix - 5 : cur_fault_ix + 6] = True
    else:
        to_print_mask = np.ones(progress_df.shape[0], dtype=bool)

    template_fmt = "{:<15}{:<12}{:<12}{:<12}{:<12}{:<12}"
    print(
        template_fmt.format(
            "Fault name", "EMOD3D", "HF", "BB", "Total", "Completed (realisations)"
        )
    )
    for fault in progress_df.index.values[to_print_mask]:
        print(
            template_fmt.format(
                fault,
                get_usage_str(fault, const.ProcessType.EMOD3D.str_value),
                get_usage_str(fault, const.ProcessType.HF.str_value),
                get_usage_str(fault, const.ProcessType.BB.str_value),
                get_usage_str(fault, "total"),
                "{}/{}".format(
                    progress_df.loc[fault, ("total", COMPLETED_R_COUNT_COL)],
                    progress_df.loc[fault, ("total", R_COUNT_COL)],
                ),
            )
        )

    lf_act = progress_df[const.ProcessType.EMOD3D.str_value, ACT_CORE_HOURS_COL].sum()
    lf_est = progress_df[const.ProcessType.EMOD3D.str_value, EST_CORE_HOURS_COL].sum()
    hf_act = progress_df[const.ProcessType.HF.str_value, ACT_CORE_HOURS_COL].sum()
    hf_est = progress_df[const.ProcessType.HF.str_value, EST_CORE_HOURS_COL].sum()
    bb_act = progress_df[const.ProcessType.BB.str_value, ACT_CORE_HOURS_COL].sum()
    bb_est = progress_df[const.ProcessType.BB.str_value, EST_CORE_HOURS_COL].sum()
    act_total = lf_act + hf_act + bb_act
    est_total = lf_est + hf_est + bb_est

    print("\nOverall progress")
    print(
        "EMOD3D: {:.2f}/{:.2f}, BB {:.2f}/{:.2f}, "
        "HF {:.2f}/{:.2f}, Total {:.2f}/{:.2f} - {:.2f}%".format(
            lf_act,
            lf_est,
            hf_act,
            hf_est,
            bb_act,
            bb_est,
            act_total,
            est_total,
            (act_total / est_total) * 100,
        )
    )

    total_r_completed = progress_df["total", COMPLETED_R_COUNT_COL].sum()
    total_r_count = progress_df["total", R_COUNT_COL].sum()

    print(
        "Number of realisations completed: {}/{} - {:.2f}%".format(
            total_r_completed,
            total_r_count,
            (total_r_completed/total_r_count) * 100
        )
    )


def main(args: argparse.Namespace):
    root_dir = args.cybershake_root
    runs_dir = sim_struct.get_runs_dir(root_dir)

    cybershake_list = sim_struct.get_cybershake_list(root_dir)
    fault_names, r_counts = get_faults_and_r_count(cybershake_list)

    # Sort, as faults are run in alphabetical order
    sort_ind = np.argsort(fault_names)
    fault_names, r_counts = fault_names[sort_ind], r_counts[sort_ind]

    faults = np.asarray(
        [sim_struct.get_fault_dir(root_dir, fault_name) for fault_name in fault_names]
    )

    if not os.path.isdir(root_dir) or not os.path.isdir(runs_dir):
        print("Not a valid cybershake root directory. Quitting!")
        return

    # Check if progress df exists
    if args.temp_file is not None and os.path.isfile(args.temp_file):
        print("Loading existing progress dataframe")

        progress_df = load_progress_df(args.temp_file)

        # Sanity check
        if np.any(progress_df.index.values != fault_names):
            raise Exception(
                "The fault names of the progress df and directories "
                "are not matching. These have to match, quitting!"
            )

        to_check_mask = (
            (
                progress_df["total", COMPLETED_R_COUNT_COL].values
                != progress_df["total", R_COUNT_COL].values
            )
            & (progress_df["total", COMPLETED_R_COUNT_COL].values > 0)
        ).ravel()

        # Find last fault that completed
        completed_fault_ind = np.flatnonzero(~to_check_mask)
        cur_fault_ix = (
            completed_fault_ind.max() if completed_fault_ind.shape[0] > 0 else 0
        )

        # Rerun all fault that are not complete + the next 5 faults from
        # the last complete one
        to_check_mask[cur_fault_ix + 1 : cur_fault_ix + 6] = True

        chours_df = get_chours_used(faults[to_check_mask])

        # Update the progress df
        total = None
        for proc_type in PROCESS_TYPES:
            values = chours_df[proc_type]
            progress_df.loc[to_check_mask, (proc_type, ACT_CORE_HOURS_COL)] = values
            total = values.values if total is None else total + values.values
        progress_df.loc[to_check_mask, ("total", ACT_CORE_HOURS_COL)] = total
        progress_df.loc[to_check_mask, ("total", COMPLETED_R_COUNT_COL)] = chours_df[
            COMPLETED_R_COUNT_COL
        ]
        progress_df.loc[to_check_mask, ("total", MISSING_DATA_FLAG_COL)] = chours_df[
            MISSING_DATA_FLAG_COL
        ]

    # Create new progress df
    else:
        progress_df = get_new_progress_df(
            root_dir, runs_dir, faults, fault_names, r_counts
        )
        cur_fault_ix = None

    # Save the progress df if a temp file is specified
    if args.temp_file is not None:
        progress_df.to_csv(args.temp_file)

    # Print the progress df
    print_progress(progress_df, cur_fault_ix)


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
