#!/usr/bin/env python3
"""Prints out the current cybershake status.

To reduce re-collection of the same metadata over and over again a csv is created
 which stores the  progress dataframe.

Note: Assumes that faults are run in alphabetical order!

The missing data column of the progress dataframe is set to True if any
metadata for any realisation of that fault.
"""
import argparse
import json
import glob
from pathlib import Path
from typing import List, Iterable
import sys


import numpy as np
import pandas as pd

import sqlite3
from urllib.request import urlopen
import yaml

from estimation.estimate_cybershake import main as est_cybershake
import qcore.simulation_structure as sim_struct
import qcore.constants as const


# The process types that are used for progress tracking
PROCESS_TYPES = [
    const.ProcessType.EMOD3D.str_value,
    const.ProcessType.HF.str_value,
    const.ProcessType.BB.str_value,
]

EST_CORE_HOURS_COL = "est_core_hours"
ACT_CORE_HOURS_COL = "act_core_hours"

R_COUNT_COL = "r_count"

SLACK_ALERT_CONFIG = "slack_alert.yaml"


def get_chours(json_file, proc_types: List[str], debug=False):
    """Gets the core hours for the specified process types (i.e. EMOD3D, HF, BB, ...)
    for the specified simulation metadata json log file.
    """
    with open(json_file, "r") as f:
        data = json.load(f)
    sim_name = data.get(const.MetadataField.sim_name.value)
    core_hours = []
    completed_proc = {}
    for proc_type in proc_types:
        if proc_type in data.keys():
            core_hours.append(
                data[proc_type].get(const.MetadataField.run_time.value, np.nan)
                * data[proc_type].get(const.MetadataField.n_cores.value, np.nan)
                / 3600.0
            )

        else:
            core_hours.append(np.nan)
    return sim_name, core_hours


def get_chours_used(fault_dirs: Iterable[str]):
    """Returns a dataframe containing the core hours used for each of the
    specified faults"""
    faults, core_hours, completed_r_counts, missing_data = [], [], [], []
    for fault in fault_dirs:
        fault = Path(fault)
        fault_name = fault.stem
        cur_fault_chours = np.asarray(
            [
                get_chours(json_file, PROCESS_TYPES)[1]
                for json_file in fault.glob(f"**/{const.METADATA_LOG_FILENAME}")
            ]
        ).reshape(-1, len(PROCESS_TYPES))

        faults.append(fault_name)
        if cur_fault_chours.shape[0] > 0:
            core_hours.append(np.nansum(cur_fault_chours, axis=0))
        else:
            core_hours.append([np.nan, np.nan, np.nan])

    df = pd.DataFrame(index=faults, data=core_hours, columns=PROCESS_TYPES)

    return df


def get_completed_r_count(db_file, proc_type: str, fault_name: str = None):
    """Returns the number of realisations of a fault "fault_name" that completed the proc_type. If fault_name is None,
    consider all faults """
    proc_type_int = const.ProcessType[proc_type].value
    conn = sqlite3.connect(str(db_file))
    c = conn.cursor()
    if fault_name is None:
        c.execute(
            f"SELECT COUNT(*) FROM state WHERE proc_type=? and status={const.Status['completed'].value}",
            (proc_type_int,),
        )
    else:
        c.execute(
            f"SELECT COUNT(*) FROM state WHERE proc_type=? and status={const.Status['completed'].value} and run_name LIKE ?",
            (proc_type_int, fault_name + "_REL%"),
        )

    completed_r_count = c.fetchone()[0]
    conn.close()
    return completed_r_count


def get_faults_and_r_count(cybershake_list: str):
    """Gets the fault names and number of realisations from a cybershake fault list."""
    with open(cybershake_list, "r") as f:
        lines = f.readlines()

    fault_names, r_counts = [], []
    for ix, line in enumerate(lines):
        line_l = line.split(" ")
        fault_names.append(line_l[0].strip())

        try:
            r_counts.append(int(line_l[1].strip().rstrip("RrNnKk")))
        except ValueError:
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
        root_yaml=None,
        output=None,
        verbose=False,
        models_dir=None,
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

    return progress_df


def load_progress_df(file: str):
    return pd.read_csv(file, index_col=[0], header=[0, 1])


def print_progress(
    progress_df: pd.DataFrame, outfile: str, db_file: str, cur_fault_ix: int = None
):
    """Prints the progress dataframe in a nice format.

    If cur_fault_ix is specified then all uncompleted faults are printed and
    the latest completed fault with the previous and next five faults.

    Otherwise all faults are printed.
    """

    f = open(outfile, "w")

    progress_df.to_csv("progress_df.csv")

    def get_usage_str(fault: str, proc_type: str):
        return "{:.2f}/{:.2f}".format(
            progress_df.loc[fault, (proc_type, ACT_CORE_HOURS_COL)],
            progress_df.loc[fault, (proc_type, EST_CORE_HOURS_COL)],
        )

    to_print_mask = np.ones(progress_df.shape[0], dtype=bool)

    template_fmt = "{:<15}{:<22}{:<7}{:<3}{:<17}{:<7}{:<3}{:<17}{:<7}{:<3}{:<22}"
    f.write(
        template_fmt.format(
            "Fault name",
            "LF time",
            "rels",
            "C",
            "HF time",
            "rels",
            "C",
            "BB time",
            "rels",
            "C",
            "Total",
        )
    )
    f.write("\n")

    est_completed_all = {}
    for fault in progress_df.index.values[to_print_mask]:
        est_completed_all[fault] = {}
        num_all_rels = int(
            progress_df.loc[fault, ("total", R_COUNT_COL)]
        )  # not sure why this is np.float64
        num_complete_rels = []
        usage_str_list = []
        is_complete = {}
        for proc_type in PROCESS_TYPES:
            usage_str_list.append(get_usage_str(fault, proc_type))
            num_complete = get_completed_r_count(db_file, proc_type, fault)
            num_complete_rels.append(f"{num_complete}/{num_all_rels}")
            is_complete[proc_type] = 1 if num_complete == num_all_rels else 0

        output_list = np.array(
            list(zip(usage_str_list, num_complete_rels, is_complete.values()))
        ).flatten()
        f.write(template_fmt.format(fault, *output_list, get_usage_str(fault, "total")))
        f.write("\n")

        for proc_type in PROCESS_TYPES:
            if is_complete[proc_type]:
                est_completed_all[fault][proc_type] = progress_df.loc[
                    fault, (proc_type, EST_CORE_HOURS_COL)
                ]

    est_completed_df = pd.DataFrame.from_dict(
        est_completed_all, orient="index", columns=PROCESS_TYPES
    )

    overall_df = progress_df.sum()

    completed_r_counts = {}
    for proc_type in PROCESS_TYPES:
        completed_r_counts[proc_type] = get_completed_r_count(db_file, proc_type)

    #    total_r_completed = progress_df["total", COMPLETED_R_COUNT_COL].sum()
    total_r_count = progress_df["total", R_COUNT_COL].sum()

    f.write("\nOverall progress\n")
    template_fmt = "{:<6} : {:>5}/{:>5} ({:4.2f}%) RELs complete: {:10.2f} CH consumed / {:10.2f} CH est ({: >#06.2f}%) {:10.2f} CH remaining est\n"
    for proc_type in PROCESS_TYPES:
        f.write(
            template_fmt.format(
                proc_type,
                completed_r_counts[proc_type],
                total_r_count,
                completed_r_counts[proc_type] / total_r_count * 100,
                overall_df[proc_type, ACT_CORE_HOURS_COL],
                overall_df[proc_type, EST_CORE_HOURS_COL],
                overall_df[proc_type, ACT_CORE_HOURS_COL]
                / overall_df[proc_type, EST_CORE_HOURS_COL]
                * 100,
                overall_df[proc_type, EST_CORE_HOURS_COL]
                - est_completed_df[proc_type].sum(),
            )
        )

    f.close()


def send2slack(msg_file, users, url):
    with open(msg_file, "r") as f:
        lines = f.readlines()

    summary = (
        " ".join(users) + " " + "\n".join(lines[-4:])
    )  # .encode('utf-8') #last 4 lines
    print(summary)
    data = json.dumps({"text": summary})
    urlopen(url, data=data.encode("utf-8"))


def main(root_dir: Path, cybershake_list, outfile, slack_config=None):

    runs_dir = Path(sim_struct.get_runs_dir(root_dir))

    fault_names, r_counts = get_faults_and_r_count(cybershake_list)

    # Sort, as faults are run in alphabetical order
    sort_ind = np.argsort(fault_names)
    fault_names, r_counts = fault_names[sort_ind], r_counts[sort_ind]

    faults = np.asarray(
        [sim_struct.get_fault_dir(root_dir, fault_name) for fault_name in fault_names]
    )

    if not root_dir.is_dir() or not runs_dir.is_dir():
        print("Not a valid cybershake root directory. Quitting!")
        return

    # Create new progress df
    progress_df = get_new_progress_df(root_dir, runs_dir, faults, fault_names, r_counts)
    cur_fault_ix = None

    db_file = root_dir / "slurm_mgmt.db"
    # Print the progress df
    print_progress(progress_df, outfile, db_file, cur_fault_ix)

    if slack_config is not None:
        send2slack(outfile, slack_config["users"], slack_config["url"])


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "cybershake_root", type=Path, help="The cybershake root directory"
    )
    parser.add_argument("list", type=str, help="The list of faults to run")
    parser.add_argument("outfile", type=Path, help="output file")

    parser.add_argument("--alert", help="send the output to slack", action="store_true")

    args = parser.parse_args()

    slack_config = None
    slack_config_path = Path(__file__).resolve().parent / SLACK_ALERT_CONFIG
    if args.alert:
        try:
            with open(slack_config_path) as f:
                slack_config = yaml.load(f, Loader=yaml.loader.SafeLoader)
        except:
            print(f"Error: --alert option used with no {slack_config_path}. Exiting")
            sys.exit(1)

    main(args.cybershake_root, args.list, args.outfile, slack_config)
