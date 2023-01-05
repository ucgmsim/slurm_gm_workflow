#!/usr/bin/env python3
"""
This script is used after the workflow has been run to collect metadata and compile it all to a csv
"""
import argparse

import numpy as np
import pandas as pd

from qcore import utils, simulation_structure, shared
from qcore import constants as const
from workflow.automation.lib import MgmtDB, constants

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
    "IM_calc_components",
    "IM_calc_FAS_count",
    "IM_calc_cores",
    "IM_calc_core_hours",
    "IM_calc_resubmits",
    "advanced_IM_runtime",
    "advanced_IM_models",
    "advanced_IM_stations",
    "advanced_IM_cores",
    "advanced_IM_core_hours",
    "advanced_IM_resubmits",
    "Total_core_hours",
]
GENERAL_PARAM_LOCATIONS = {
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
METADATA_PROC_TYPES = [
    const.ProcessType.EMOD3D.value,
    const.ProcessType.VM_GEN.value,
    const.ProcessType.VM_PERT.value,
    const.ProcessType.HF.value,
    const.ProcessType.BB.value,
    const.ProcessType.IM_calculation.value,
    const.ProcessType.advanced_IM.value,
]


class DbStat:
    def __init__(self, low: float, high: float):
        self.low = low
        self.high = high

    def add_value(self, new_val: float):
        if self.low > new_val:
            self.low = new_val
        elif self.high < new_val:
            self.high = new_val

    def __str__(self):
        return f"{self.low}-{self.high}"


def add_db_stat(df: pd.DataFrame, rel_name: str, df_location: str, new_val: int):
    """
    Adds the db stat to the dataframe
    But first checks if there is a current non_zero value in the df location
    If there is one then it adds the values as a low-high format
    """
    prev_val = df.loc[rel_name, df_location]
    if prev_val != 0:
        if isinstance(prev_val, DbStat):
            prev_val.add_value(new_val)
        elif prev_val != new_val:
            if prev_val > new_val:
                low, high = new_val, prev_val
            else:
                low, high = prev_val, new_val
            df.loc[rel_name, df_location] = DbStat(low, high)
    else:
        df.loc[rel_name, df_location] = new_val
    return df


def get_rel_info(
    rel_name: str,
    root_dir: str,
    db: MgmtDB,
    ch_count_type: constants.ChCountType,
):
    """
    Loads the given relisations info and populates the dataframe row
    """
    fault_name = simulation_structure.get_fault_from_realisation(rel_name)
    params = utils.load_sim_params(
        simulation_structure.get_sim_params_yaml_path(
            f"{root_dir}/Runs/{fault_name}/{rel_name}"
        ),
        load_vm=True,
    )

    # Create relisation df
    df = pd.DataFrame(
        columns=COLUMNS, index=[rel_name], data=np.zeros(shape=(1, len(COLUMNS)))
    )

    # General Parameter Metadata
    for k, v in GENERAL_PARAM_LOCATIONS.items():
        if isinstance(v, list):
            value = params
            for index in v:
                value = value[index]
        else:
            value = params[v]
        df.loc[rel_name, k] = value

    states = db.get_core_hour_states(rel_name, ch_count_type)
    resub_counter = dict()
    # DB Metadata
    for state in states:
        # Get proc_type and job_id
        _, _, proc_type, status, job_id, _ = state
        proc_type_name = const.ProcessType(proc_type).str_value
        if proc_type in METADATA_PROC_TYPES:
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
            ) = db.get_job_duration_info(job_id)
            # Add runtime and cores to df
            runtime = end_time - start_time
            df.loc[rel_name, f"{proc_type_name}_runtime"] += runtime / 60
            df = add_db_stat(df, rel_name, f"{proc_type_name}_cores", cores)
            # Add to core hours
            df.loc[rel_name, f"{proc_type_name}_core_hours"] += cores * runtime / 3600
            df.loc[rel_name, f"Total_core_hours"] += cores * runtime / 3600
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
    df.loc[rel_name, "HF_nt"] = params["sim_duration"] / params["hf"]["dt"]
    df.loc[rel_name, "BB_nt"] = params["sim_duration"] / params["bb"]["dt"]
    df.loc[rel_name, "advanced_IM_models"] = len(params["advanced_IM"]["models"])
    stations = shared.get_stations(params["stat_file"])
    df.loc[rel_name, "HF_n_stats"] = len(stations)
    df.loc[rel_name, "BB_n_stats"] = len(stations)
    df.loc[rel_name, "advanced_IM_stations"] = len(stations)

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


def parse_args():
    """
    Parses the arguments
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("root_dir", type=str, help="The root directory")
    parser.add_argument(
        "ch_count_type",
        type=str,
        choices=constants.ChCountType.get_names(),
        help="How to count the Core Hours, 'Actual' counts the actual core hours used."
        " 'Needed' counts the core hours it should have used without fails",
    )
    parser.add_argument("output_ffp", type=str)
    return parser.parse_args()


def main():
    """
    Gather metadata from each realisation and outputs to a csv
    """
    # Get arguments
    args = parse_args()
    root_dir, output_ffp = args.root_dir, args.output_ffp
    ch_count_type = constants.ChCountType[args.ch_count_type]

    # Generate dataframe
    db = MgmtDB.MgmtDB(f"{root_dir}/slurm_mgmt.db")
    rel_names = db.get_rel_names()
    df = pd.DataFrame(
        columns=COLUMNS, data=np.zeros(shape=(len(rel_names), len(COLUMNS)))
    )
    df.index = [name_tuple[0] for name_tuple in rel_names]
    df.index.name = "Rel_name"
    for ix, name_tuple in enumerate(rel_names):
        rel_name = name_tuple[0]
        df.loc[rel_name] = get_rel_info(rel_name, root_dir, db, ch_count_type).loc[
            rel_name
        ]
    df.to_csv(output_ffp)


if __name__ == "__main__":
    main()
