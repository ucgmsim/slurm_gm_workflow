#!/usr/bin/env python3
"""Prints out the current cybershake status.
"""
import argparse
import json
from pathlib import Path
from typing import List

import numpy as np
import pandas as pd
import sqlite3 as sql
from urllib.request import urlopen


from workflow.automation.estimation.estimate_cybershake import main as est_cybershake
from workflow.automation.lib.MgmtDB import MgmtDB
import qcore.simulation_structure as sim_struct
import qcore.constants as const
from qcore.formats import load_fault_selection_file
from qcore.utils import load_yaml

# The process types that are used for progress tracking
PROCESS_TYPES = [
    const.ProcessType.EMOD3D.str_value,
    const.ProcessType.HF.str_value,
    const.ProcessType.BB.str_value,
]

EST_CORE_HOURS_COL = "est_core_hours"
ACT_CORE_HOURS_COL = "act_core_hours"
NUM_COMPLETED_COL = "num_completed"

R_COUNT_COL = "r_count"

SLACK_ALERT_CONFIG = "slack_alert.yaml"

_fault_template_fmt = "{:<15}{:<22}{:<7}{:<3}{:<17}{:<7}{:<3}{:<17}{:<7}{:<3}{:<22}"
_summary_template_fmt = "{:<6} : {:>5}/{:>5} ({:4.2f}%) RELs complete: {:10.2f} CH consumed / {:10.2f} CH est ({: >#06.2f}%) {:10.2f} CH remaining est"


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


def get_chours_used(root_dir: str):
    """Returns a dataframe containing the core hours used for each of the
    specified faults"""
    db = sql.connect(f"{root_dir}/slurm_mgmt.db")

    # faults, core_hours, completed_r_counts, missing_data = [], [], [], []
    # for fault in fault_dirs:
    #     fault = Path(fault)
    #     fault_name = fault.stem
    #     cur_fault_chours = np.asarray(
    #         [
    #             get_chours(json_file, PROCESS_TYPES)[1]
    #             for json_file in fault.glob(f"**/{const.METADATA_LOG_FILENAME}")
    #         ]
    #     ).reshape(-1, len(PROCESS_TYPES))
    #
    #     faults.append(fault_name)
    #     if cur_fault_chours.shape[0] > 0:
    #         core_hours.append(np.nansum(cur_fault_chours, axis=0))
    #     else:
    #         core_hours.append([np.nan, np.nan, np.nan])
    #
    # df = pd.DataFrame(index=faults, data=core_hours, columns=PROCESS_TYPES)

    return df


def get_faults_dict(cybershake_list: str):
    """Gets the fault names and number of realisations from a cybershake fault list."""
    faults_dict = load_fault_selection_file(cybershake_list)
    return faults_dict


def get_new_progress_df(root_dir, runs_dir, faults_dict, mgmtdb: MgmtDB):
    """Gets a new progress dataframe, runs the full estimation + collects
    all actual core hours and number of completed realisations
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

    fault_names, r_counts = np.asarray(list(faults_dict.keys())), np.asarray(
        list(faults_dict.values())
    )
    sort_ind = np.argsort(fault_names)
    fault_names, r_counts = fault_names[sort_ind], r_counts[sort_ind]

    # Sanity check
    assert np.all(
        grouped_df.index.values == fault_names
    ), "The fault names of the estimation results and those from the list aren't matching"

    # Create progress dataframe
    column_t = []
    for proc_type in PROCESS_TYPES:
        column_t.append((proc_type, EST_CORE_HOURS_COL))
        column_t.append((proc_type, ACT_CORE_HOURS_COL))
        column_t.append((proc_type, NUM_COMPLETED_COL))
    column_t.append(("total", EST_CORE_HOURS_COL))
    column_t.append(("total", ACT_CORE_HOURS_COL))
    column_t.append(("total", NUM_COMPLETED_COL))
    column_t.append(("total", R_COUNT_COL))

    progress_df = pd.DataFrame(
        index=grouped_df.index.values, columns=pd.MultiIndex.from_tuples(column_t)
    )

    # Get actual core hours for all faults
    fault_dirs = np.asarray(
        [sim_struct.get_fault_dir(root_dir, fault_name) for fault_name in fault_names]
    )
    chours_df = get_chours_used(root_dir)

    # Populate progress dataframe with estimation data and actual data
    for proc_type in PROCESS_TYPES:
        progress_df[proc_type, EST_CORE_HOURS_COL] = grouped_df[
            proc_type, const.MetadataField.core_hours.value
        ]
        progress_df[proc_type, ACT_CORE_HOURS_COL] = chours_df[proc_type]

    # Retrieve the number of completed RELs from DB
    for fault_name in fault_names:
        for proc_type in PROCESS_TYPES:
            r_completed = mgmtdb.num_task_complete(
                (const.ProcessType[proc_type].value, fault_name + "_REL%"), like=True
            )
            progress_df.loc[fault_name, (proc_type, NUM_COMPLETED_COL)] = r_completed

    # Compute total estimated time and actual time across all faults
    idx = pd.IndexSlice
    progress_df["total", EST_CORE_HOURS_COL] = progress_df.loc[
        :, idx[:, EST_CORE_HOURS_COL]
    ].sum(axis=1)
    progress_df["total", ACT_CORE_HOURS_COL] = progress_df.loc[
        :, idx[:, ACT_CORE_HOURS_COL]
    ].sum(axis=1)
    progress_df["total", NUM_COMPLETED_COL] = progress_df.loc[
        :, idx[:, NUM_COMPLETED_COL]
    ].min(axis=1)
    progress_df["total", R_COUNT_COL] = r_counts

    return progress_df


def load_progress_df(file: str):
    return pd.read_csv(file, index_col=[0], header=[0, 1])


def print_progress(progress_df: pd.DataFrame):
    """Prints the progress dataframe to screen (in a nice format)."""

    def get_usage_str(fault_name: str, proc_type: str):
        return "{:.2f}/{:.2f}".format(
            progress_df.loc[fault_name, (proc_type, ACT_CORE_HOURS_COL)],
            progress_df.loc[fault_name, (proc_type, EST_CORE_HOURS_COL)],
        )

    print(
        _fault_template_fmt.format(
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

    est_completed_dict = {}

    for fault_name in progress_df.index.values:
        est_completed_dict[fault_name] = {}
        r_count = int(
            progress_df.loc[fault_name, ("total", R_COUNT_COL)]
        )  # not sure why this is np.float64
        r_completed_str = {}
        usage_str = {}
        is_complete = {}
        for proc_type in PROCESS_TYPES:
            usage_str[proc_type] = get_usage_str(fault_name, proc_type)
            r_completed = int(
                progress_df.loc[fault_name, (proc_type, NUM_COMPLETED_COL)]
            )
            r_completed_str[proc_type] = f"{r_completed}/{r_count}"
            is_complete[proc_type] = 1 if r_completed == r_count else 0
            if is_complete[proc_type]:
                est_completed_dict[fault_name][proc_type] = progress_df.loc[
                    fault_name, (proc_type, EST_CORE_HOURS_COL)
                ]  # estimated time for a completed "fault_name" and "proc_type"

        # arrange the data for printing
        output_list = np.array(
            list(
                zip(usage_str.values(), r_completed_str.values(), is_complete.values())
            )
        ).flatten()
        print(
            _fault_template_fmt.format(
                fault_name, *output_list, get_usage_str(fault_name, "total")
            )
        )

    est_completed_df = pd.DataFrame.from_dict(
        est_completed_dict, orient="index", columns=PROCESS_TYPES
    )

    overall_df = progress_df.sum()

    total_r_count = overall_df["total", R_COUNT_COL]
    total_complete_rels = {}
    for proc_type in PROCESS_TYPES:
        total_complete_rels[proc_type] = int(
            progress_df[proc_type, NUM_COMPLETED_COL].sum()
        )

    summary = "\nOverall progress\n"
    for proc_type in PROCESS_TYPES:
        line = _summary_template_fmt.format(
            proc_type,
            total_complete_rels[proc_type],
            total_r_count,
            total_complete_rels[proc_type] / total_r_count * 100,
            overall_df[proc_type, ACT_CORE_HOURS_COL],
            overall_df[proc_type, EST_CORE_HOURS_COL],
            overall_df[proc_type, ACT_CORE_HOURS_COL]
            / overall_df[proc_type, EST_CORE_HOURS_COL]
            * 100,
            overall_df[proc_type, EST_CORE_HOURS_COL]
            - est_completed_df[proc_type].sum(),
        )
        summary += line + "\n"

    print(summary)
    return summary


def send2slack(msg, users, url):
    summary = " ".join(users) + "\n" + msg  # .encode('utf-8')
    data = json.dumps({"text": summary})
    urlopen(url, data=data.encode("utf-8"))


def main(root_dir: Path, cybershake_list, slack_config=None):
    runs_dir = Path(sim_struct.get_runs_dir(root_dir))
    assert (
        root_dir.is_dir() and runs_dir.is_dir()
    ), f"Error: {root_dir} is not a valid cybershake root directory"

    faults_dict = get_faults_dict(cybershake_list)

    mgmtdb = MgmtDB(sim_struct.get_mgmt_db(root_dir))

    # Create new progress df
    progress_df = get_new_progress_df(root_dir, runs_dir, faults_dict, mgmtdb)

    summary = print_progress(progress_df)

    if slack_config is not None:
        send2slack(summary, slack_config["users"], slack_config["url"])


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "cybershake_root", type=Path, help="The cybershake root directory"
    )
    parser.add_argument("list", type=str, help="The list of faults to run")
    parser.add_argument("--alert", help="send the output to slack", action="store_true")

    args = parser.parse_args()

    slack_config = None
    slack_config_path = Path(__file__).resolve().parent / SLACK_ALERT_CONFIG
    if args.alert:
        assert (
            slack_config_path.exists()
        ), f"Error: --alert option requires {slack_config_path} to be present."
        slack_config = load_yaml(slack_config_path)

    main(args.cybershake_root, args.list, slack_config)
