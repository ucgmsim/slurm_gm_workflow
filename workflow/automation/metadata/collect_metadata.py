#!/usr/bin/env python3
"""
This script is used after the workflow has been run to collect metadata and compile it all to a csv
"""
import os
import argparse

import sqlite3 as sql
import numpy as np
import pandas as pd

from qcore import utils
from qcore import constants as const
from workflow.automation.lib.shared import get_stations

COLUMNS = [
    "EMOD3D_runtime",
    "EMOD3D_nx",
    "EMOD3D_ny",
    "EMOD3D_nz",
    "EMOD3D_nt",
    "EMOD3D_min_Vs",
    "EMOD3D_flo",
    "EMOD3D_cores",
    "EMOD3D_core_hours",
    "EMOD3D_resubmits",
    "VM_GEN_runtime",
    "VM_GEN_nx",
    "VM_GEN_ny",
    "VM_GEN_nz",
    "VM_GEN_flo",
    "VM_GEN_cores",
    "VM_GEN_core_hours",
    "VM_GEN_resubmits",
    "VM_PERT_runtime",
    "VM_PERT_nx",
    "VM_PERT_ny",
    "VM_PERT_nz",
    "VM_PERT_flo",
    "VM_PERT_cores",
    "VM_PERT_core_hours",
    "VM_PERT_resubmits",
    "HF_runtime",
    "HF_n_stats",
    "HF_dt",
    "HF_nt",
    "HF_cores",
    "HF_core_hours",
    "HF_resubmits",
    "BB_runtime",
    "BB_n_stats",
    "BB_dt",
    "BB_nt",
    "BB_cores",
    "BB_core_hours",
    "BB_resubmits",
    "IM_calc_runtime",
    "IM_calc_pSA_count",
    "IM_calc_BB_nt",
    "IM_calc_components",
    "IM_calc_FAS_count",
    "IM_calc_cores",
    "IM_calc_core_hours",
    "IM_calc_resubmits",
    "AdvIM_runtime",
    "AdvIM_models",
    "AdvIM_stations",
    "AdvIM_cores",
    "AdvIM_core_hours",
    "AdvIM_resubmits",
]
METADATA_PROC_TYPES = ["EMOD3D", "VM_GEN", "VM_PERT", "HF", "BB", "IM_calc", "AdvIM"]


def add_db_stat(df: pd.DataFrame, rel_name: str, df_location: str, new_val: int):
    """
    Adds the db stat to the dataframe
    But first checks if there is a current non_zero value in the df location
    If there is one then it adds the values as a low-high format
    """
    if df.loc[rel_name, df_location] != 0:
        prev_runtime = df.loc[rel_name, df_location]
        if isinstance(prev_runtime, str) and "-" in prev_runtime:
            low, high = prev_runtime.split("-")
            if low > new_val:
                low = new_val
            elif high < new_val:
                high = new_val
            df.loc[rel_name, df_location] = f"{low}-{high}"
        elif prev_runtime > new_val:
            low, high = new_val, prev_runtime
            df.loc[rel_name, df_location] = f"{low}-{high}"
        elif prev_runtime < new_val:
            low, high = prev_runtime, new_val
            df.loc[rel_name, df_location] = f"{low}-{high}"
    else:
        df.loc[rel_name, df_location] = new_val
    return df


def get_rel_info(df: pd.DataFrame, rel_name: str, root_dir: str, db: sql.Connection):
    """
    Loads the given relisations info and populates the dataframe row
    """
    fault_name = rel_name.split("_")[0] if "REL" in rel_name else rel_name
    params = utils.load_sim_params(
        os.path.join(f"{root_dir}/Runs/{fault_name}/{rel_name}", "sim_params.yaml"),
        load_vm=True,
    )

    # General Parameter Metadata
    general_param_locations = {
        "EMOD3D_nx": "nx",
        "EMOD3D_ny": "ny",
        "EMOD3D_nz": "nz",
        "EMOD3D_min_Vs": "min_vs",
        "EMOD3D_flo": "flo",
        "VM_GEN_nx": "nx",
        "VM_GEN_ny": "ny",
        "VM_GEN_nz": "nz",
        "VM_GEN_flo": "flo",
        "VM_PERT_nx": "nx",
        "VM_PERT_ny": "ny",
        "VM_PERT_nz": "nz",
        "VM_PERT_flo": "flo",
        "HF_dt": ["hf", "dt"],
        "BB_dt": ["bb", "dt"],
    }
    for k, v in general_param_locations.items():
        if isinstance(v, list):
            value = params
            for index in v:
                value = value[index]
        else:
            value = params[v]
        df.loc[rel_name, k] = value

    # DB Metadata
    states = db.execute(
        "SELECT * from state WHERE run_name=? AND status != ?",
        (rel_name, const.Status.created.value),
    ).fetchall()
    resub_counter = dict()
    for state in states:
        # Get proc_type and job_id
        _, _, proc_type, status, job_id, _ = state
        proc_type_name = db.execute(
            "SELECT proc_type from proc_type_enum where id=?",
            (proc_type,),
        ).fetchone()[0]
        proc_type_name = (
            "IM_calc" if proc_type_name == "IM_calculation" else proc_type_name
        )
        if proc_type_name in METADATA_PROC_TYPES:
            (
                _,
                _,
                queued_time,
                start_time,
                end_time,
                nodes,
                cores,
                memory,
                WCT,
            ) = db.execute(
                "SELECT * from job_duration_log WHERE job_id=?",
                (job_id,),
            ).fetchone()
            # Add runtime and cores to df
            runtime = end_time - start_time
            df = add_db_stat(df, rel_name, f"{proc_type_name}_runtime", runtime)
            df = add_db_stat(df, rel_name, f"{proc_type_name}_cores", cores)
            # Add to core hours
            df.loc[rel_name, f"{proc_type_name}_core_hours"] += cores * runtime / 60
            # Add to resubmits dict
            if resub_counter.get(proc_type_name) is None:
                resub_counter[proc_type_name] = 0
            else:
                resub_counter[proc_type_name] += 1
    # Add resubmits to df
    for k, v in resub_counter.items():
        df.loc[rel_name, f"{k}_resubmits"] = v

    # Extra Metadata
    df.loc[rel_name, "IM_calc_components"] = len(params["ims"]["component"])
    df.loc[rel_name, "EMOD3D_nt"] = params["sim_duration"] / params["dt"]
    df.loc[rel_name, "HF_nt"] = params["sim_duration"] / params["dt"]
    df.loc[rel_name, "BB_nt"] = params["sim_duration"] / params["dt"]
    df.loc[rel_name, "IM_calc_BB_nt"] = params["sim_duration"] / params["dt"]
    df.loc[rel_name, "AdvIM_models"] = len(params["advanced_IM"]["models"])
    stations = get_stations(params["stat_file"])
    df.loc[rel_name, "HF_n_stats"] = len(stations)
    df.loc[rel_name, "BB_n_stats"] = len(stations)
    df.loc[rel_name, "AdvIM_stations"] = len(stations)

    # From IM calc csv
    csv_ffp = f"{root_dir}/Runs/{fault_name}/{rel_name}/IM_calc/{rel_name}.csv"
    try:
        csv_columns = pd.read_csv(csv_ffp).columns.values
        df.loc[rel_name, "IM_calc_pSA_count"] = len(
            [True for column in csv_columns if "pSA" in column]
        )
        df.loc[rel_name, "IM_calc_FAS_count"] = len(
            [True for column in csv_columns if "FAS" in column]
        )
    except FileNotFoundError:
        print(f"Could not find file {csv_ffp} (Will not exist if IM_calc was not run)")
    return df


def main(root_dir: str, out_ffp: str):
    """
    Gather metadata from each realisation and outputs to a csv
    """
    db = sql.connect(f"{root_dir}/slurm_mgmt.db")
    rel_names = db.execute("SELECT DISTINCT run_name from state").fetchall()
    df = pd.DataFrame(
        columns=COLUMNS, data=np.zeros(shape=(len(rel_names), len(COLUMNS)))
    )
    df.index = [name_tuple[0] for name_tuple in rel_names]
    df.index.name = "Rel_name"
    for ix, name_tuple in enumerate(rel_names):
        df = get_rel_info(df, name_tuple[0], root_dir, db)
    db.close()
    df.to_csv(out_ffp)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("root_dir", type=str, help="The root directory")
    parser.add_argument("output_ffp", type=str)
    args = parser.parse_args()
    main(args.root_dir, args.output_ffp)
