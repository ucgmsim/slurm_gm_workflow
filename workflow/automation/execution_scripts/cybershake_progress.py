#!/usr/bin/env python3
"""Prints out the current cybershake status.
"""
import argparse
import json
from pathlib import Path
from typing import List

import numpy as np
import pandas as pd
from urllib.request import urlopen

from workflow.automation.estimation.estimate_cybershake import main as est_cybershake
from workflow.automation.lib.MgmtDB import MgmtDB, ComparisonOperator
from workflow.automation.lib.constants import ChCountType
import qcore.simulation_structure as sim_struct
import qcore.constants as const
from qcore.formats import load_fault_selection_file
from qcore.utils import load_yaml

# The default process types that are used for progress tracking
DEFAULT_PROCESS_TYPES = [
    const.ProcessType.EMOD3D.str_value,
    const.ProcessType.HF.str_value,
    const.ProcessType.BB.str_value,
    const.ProcessType.IM_calculation.str_value,
]

EST_CORE_HOURS_COL = "est_core_hours"
ACT_CORE_HOURS_COL = "act_core_hours"
NUM_COMPLETED_COL = "num_completed"

R_COUNT_COL = "r_count"

SLACK_ALERT_CONFIG = "slack_alert.yaml"

_fault_template_fmt = "{:<15}{:<22}{:<7}{:<3}{:<17}{:<7}{:<3}{:<17}{:<7}{:<3}{:<22}"
_summary_con_template_fmt = (
    "{:<6} : {:>5}/{:>5} ({:4.2f}%) RELs complete: {:10.2f} CH consumed"
)
_summary_est_template_fmt = (
    " / {:10.2f} CH est ({: >#06.2f}%) {:10.2f} CH remaining est"
)


def get_chours_used(root_dir: str, fault_names: List[str], proc_types: List[str]):
    """Returns a dataframe containing the core hours used for each of the
    specified faults"""
    db = MgmtDB(f"{root_dir}/slurm_mgmt.db")
    df = pd.DataFrame(
        columns=proc_types,
        index=fault_names,
        data=np.zeros(shape=(len(fault_names), len(proc_types))),
    )

    rel_names = db.get_rel_names()
    for fault_name in fault_names:
        flt_rel_names = [
            rel_name[0] for rel_name in rel_names if fault_name in rel_name[0]
        ]
        for rel_name in flt_rel_names:
            rel_states = db.get_core_hour_states(rel_name, ChCountType.Needed)
            for state in rel_states:
                _, _, proc_type, _, job_id, _ = state
                proc_type_name = const.ProcessType(proc_type).str_value
                if proc_type_name in proc_types:
                    (
                        _,
                        _,
                        _,
                        start_time,
                        end_time,
                        _,
                        cores,
                        _,
                        _,
                    ) = db.get_job_duration_info(job_id)
                    runtime = end_time - start_time
                    df.loc[fault_name, proc_type_name] += cores * runtime / 3600
    return df


def get_faults_dict(cybershake_list: str):
    """Gets the fault names and number of realisations from a cybershake fault list."""
    faults_dict = load_fault_selection_file(cybershake_list)
    return faults_dict


def get_new_progress_df(root_dir, faults_dict, mgmtdb: MgmtDB, proc_types: List[str]):
    """Gets a new progress dataframe, runs the full estimation + collects
    all actual core hours and number of completed realisations
    """
    # Run the estimation
    df = est_cybershake(root_dir)
    grouped_df = df.groupby("fault_name").sum()
    grouped_df.sort_index(axis=0, level=0, inplace=True)

    fault_names, r_counts = np.asarray(list(faults_dict.keys())), np.asarray(
        list(faults_dict.values())
    )
    sort_ind = np.argsort(fault_names)
    fault_names, r_counts = fault_names[sort_ind], r_counts[sort_ind]

    # Ensure the grouped_df only contains faults from the faults_dict
    grouped_df = grouped_df.loc[fault_names]

    # Create progress dataframe
    column_t = []
    for proc_type in proc_types:
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
    chours_df = get_chours_used(root_dir, fault_names, proc_types)

    # Populate progress dataframe with estimation data and actual data
    for proc_type in proc_types:
        if proc_type in DEFAULT_PROCESS_TYPES:
            progress_df[proc_type, EST_CORE_HOURS_COL] = grouped_df[
                proc_type, const.MetadataField.core_hours.value
            ]
        progress_df[proc_type, ACT_CORE_HOURS_COL] = chours_df[proc_type]

    # Retrieve the number of completed RELs from DB
    for fault_name in fault_names:
        for proc_type in proc_types:
            using_rels = faults_dict[fault_name] > 1
            r_completed = mgmtdb.num_task_complete(
                (const.ProcessType.from_str(proc_type).value, fault_name), matcher=ComparisonOperator.EXACT
            )
            if using_rels:
                r_completed += mgmtdb.num_task_complete(
                    (const.ProcessType.from_str(proc_type).value, fault_name + "_REL%"), matcher=ComparisonOperator.LIKE
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


def print_progress(progress_df: pd.DataFrame, proc_types: List[str]):
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
        for proc_type in proc_types:
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
        est_completed_dict, orient="index", columns=proc_types
    )

    overall_df = progress_df.sum()

    total_r_count = overall_df["total", R_COUNT_COL]
    total_complete_rels = {}
    for proc_type in proc_types:
        total_complete_rels[proc_type] = int(
            progress_df[proc_type, NUM_COMPLETED_COL].sum()
        )

    summary = "\nOverall progress\n"
    for proc_type in proc_types:
        summary += _summary_con_template_fmt.format(
            proc_type,
            total_complete_rels[proc_type],
            total_r_count,
            total_complete_rels[proc_type] / total_r_count * 100,
            overall_df[proc_type, ACT_CORE_HOURS_COL],
        )
        if proc_type in DEFAULT_PROCESS_TYPES:
            summary += _summary_est_template_fmt.format(
                overall_df[proc_type, EST_CORE_HOURS_COL],
                overall_df[proc_type, ACT_CORE_HOURS_COL]
                / overall_df[proc_type, EST_CORE_HOURS_COL]
                * 100,
                overall_df[proc_type, EST_CORE_HOURS_COL]
                - est_completed_df[proc_type].sum(),
            )
        summary += "\n"

    print(summary)
    return summary


def send2slack(msg, users, url):
    summary = " ".join(users) + "\n" + msg  # .encode('utf-8')
    data = json.dumps({"text": summary})
    urlopen(url, data=data.encode("utf-8"))


def main(root_dir: Path, cybershake_list, proc_types, slack_config=None):
    assert (
        root_dir.is_dir()
    ), f"Error: {root_dir} is not a valid cybershake root directory"

    mgmtdb = MgmtDB(sim_struct.get_mgmt_db(root_dir))

    if cybershake_list is None:
        rel_names = mgmtdb.get_rel_names()
        faults_dict = dict()
        for rel_name_tuple in rel_names:
            flt_name = sim_struct.get_fault_from_realisation(rel_name_tuple[0])
            if flt_name in faults_dict:
                faults_dict[flt_name] += 1
            else:
                faults_dict[flt_name] = 1
    else:
        faults_dict = get_faults_dict(cybershake_list)

    # Create new progress df
    progress_df = get_new_progress_df(root_dir, faults_dict, mgmtdb, proc_types)

    summary = print_progress(progress_df, proc_types)

    if slack_config is not None:
        send2slack(summary, slack_config["users"], slack_config["url"])


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "cybershake_root", type=Path, help="The cybershake root directory"
    )
    parser.add_argument(
        "--list", type=str, default=None, help="Optional list of faults to run"
    )
    parser.add_argument(
        "--proc_types",
        type=str,
        nargs="+",
        default=DEFAULT_PROCESS_TYPES,
        help="Optional list of process types to get progress for",
    )
    parser.add_argument("--alert", help="send the output to slack", action="store_true")

    args = parser.parse_args()

    slack_config = None
    slack_config_path = Path(__file__).resolve().parent / SLACK_ALERT_CONFIG
    if args.alert:
        assert (
            slack_config_path.exists()
        ), f"Error: --alert option requires {slack_config_path} to be present."
        slack_config = load_yaml(slack_config_path)

    main(args.cybershake_root, args.list, args.proc_types, slack_config)
