#!/usr/bin/env python3
"""Script for estimating core hours and run time for LF, HH, BB
for the specified srf/vm (and runs) folder
"""
import sys
import os
import json
import pandas as pd
import numpy as np

from enum import Enum
from argparse import ArgumentParser
from typing import List

from qcore import shared, srf, utils
from estimation import estimate_wct

PARAMS_VEL_FILENAME = "params_vel.json"

# The node time threshold factor used for ncores scaling
NODE_TIME_TH_FACTOR = 0.5

# Columns names of the result data coumns
class ResultColsConst(Enum):
    core_hours = "core_hours"
    run_time = "run_time"
    ncores = "ncores"


def get_faults(vms_dir, sources_dir, runs_dir, args):
    """Gets the faults and their realisation counts.
    Handles the different cases from the specified user arguments.
    """

    def get_realisations(faults, path):
        """Gets the realisation for each of the specified faults.
        Assumes that the directories exists, no checking is done.
        """
        return np.asarray(
            [np.asarray(os.listdir(os.path.join(path, fault))) for fault in faults]
        )

    faults_df = (
        None
        if args.fault_selection is None
        else pd.read_table(
            args.fault_selection,
            header=None,
            sep="\s+",
            index_col=False,
            names=["fault_name", "r_counts"],
        )
    )

    vms_faults = np.asarray(os.listdir(vms_dir))
    sources_faults = np.asarray(os.listdir(sources_dir))
    faults = np.intersect1d(vms_faults, sources_faults)

    # No runs directory and faults selection file
    if runs_dir is None and faults_df is None:
        faults = vms_faults
    # No runs directory but the faults selection file is provided
    elif runs_dir is None and faults_df:
        mask = np.isin(faults_df["fault_name"].values, faults)

        faults = faults_df.loc[mask, "fault_name"].values
    # Runs folder is specified
    elif runs_dir is not None:
        runs_faults = np.asarray(os.listdir(runs_dir))
        # faults selection file is also provided
        if faults_df is not None:
            mask = np.isin(faults_df.fault_name.values, faults) & np.isin(
                faults_df.fault_name.values, runs_faults
            )

            faults = faults_df.loc[mask, "fault_name"].values
        # No faults selection
        else:
            faults = np.intersect1d(runs_faults, faults)

    realisations = get_realisations(faults, sources_dir)

    print(
        "Found {} faults and {} realisations".format(
            faults.shape[0], np.sum([len(cur_r_list) for cur_r_list in realisations])
        )
    )

    return faults, realisations


def get_vm_params(fault_vm_path):
    """Gets nx, ny, nz and dt from the velocity params file"""
    with open(os.path.join(fault_vm_path, PARAMS_VEL_FILENAME), "r") as f:
        params_vel_dict = json.load(f)

    return [
        params_vel_dict.get("nx", np.nan),
        params_vel_dict.get("ny", np.nan),
        params_vel_dict.get("nz", np.nan),
        params_vel_dict.get("sim_duration", np.nan),
        params_vel_dict.get("dt", np.nan),
    ]


def run_estimations(
    fault_names,
    realisations,
    r_counts,
    lf_input_data: np.ndarray,
    hf_input_data=None,
    bb_input_data=None,
):
    """Runs the estimations for LF and BB, HF (if the runs_dir argument was set)
    and returns a dataframe containing the results for
    each fault/realisation combination.
    """
    print("Running estimation for LF")
    lf_core_hours, lf_run_time, lf_ncores = estimate_wct.estimate_LF_chours(
        lf_input_data, True
    )
    lf_result_data = np.concatenate(
        (lf_core_hours[:, None], lf_run_time[:, None], lf_ncores[:, None]), axis=1
    )

    # LF estimation is done per fault, results are shown per realisation
    # "Expand" the LF estimation result to per realisation
    lf_result_data = np.repeat(lf_result_data, r_counts, axis=0)

    index = pd.MultiIndex.from_tuples(
        [
            (cur_fault, cur_realisation)
            for ix, cur_fault in enumerate(fault_names)
            for cur_realisation in realisations[ix]
        ]
    )

    lf_columns = pd.MultiIndex.from_tuples(
        [("LF", data_col.value) for data_col in ResultColsConst]
    )

    results_df = pd.DataFrame(lf_result_data, index=index, columns=lf_columns)

    if hf_input_data is not None:
        print("Running HF estimation")
        hf_core_hours, hf_run_time = estimate_wct.estimate_HF_chours(hf_input_data)
        hf_cores = hf_input_data[:, -1]
    else:
        hf_core_hours, hf_run_time, hf_cores = np.nan, np.nan, np.nan

    results_df[("HF", ResultColsConst.core_hours.value)] = hf_core_hours
    results_df[("HF", ResultColsConst.run_time.value)] = hf_run_time
    results_df[("HF", ResultColsConst.ncores.value)] = hf_cores

    if bb_input_data is not None:
        print("Running BB estimation")
        bb_core_hours, bb_run_time = estimate_wct.estimate_BB_chours(bb_input_data)
        bb_cores = bb_input_data[:, -1]
    else:
        bb_core_hours, bb_run_time, bb_cores = np.nan, np.nan, np.nan

    results_df[("BB", ResultColsConst.core_hours.value)] = bb_core_hours
    results_df[("BB", ResultColsConst.run_time.value)] = bb_run_time
    results_df[("BB", ResultColsConst.ncores.value)] = bb_cores

    return results_df


def display_results(df: pd.DataFrame, verbose: bool = False):
    """Displays the results in a nice format.
    If verbose is specified the results are shown on a per fault basis.
    """
    print()
    if verbose:
        header = "{:<12}{:<10}{:<8}".format("core hours", "run time", "cores")
        print("{:>14}{:>30}{:>30}".format("LF", "HF", "BB"))
        print("{:>12}{}{}{}".format("", header, header, header))
        for fault_name, row in df.groupby("fault_name").sum().iterrows():
            lf_str = "{:<12.3f}{:<10.3f}{:<8.0f}".format(
                row.LF.core_hours, row.LF.run_time, row.LF.ncores
            )
            hf_str = "{:<12.3f}{:<10.3f}{:<8.0f}".format(
                row.HF.core_hours, row.HF.run_time, row.HF.ncores
            )
            bb_str = "{:<12.3f}{:<10.3f}{:<8.0f}".format(
                row.BB.core_hours, row.BB.run_time, row.BB.ncores
            )
            print("{:<12}{}{}{}".format(fault_name, lf_str, hf_str, bb_str))

    print()
    sum_df = df.sum()
    header = "{:<12}{:<10}{:<8}".format("core hours", "run time", "")
    print("{:>14}{:>30}{:>30}".format("LF", "HF", "BB"))
    print("{:>12}{}{}{}".format("", header, header, header))
    print(
        "{:<12}{:<12.3f}{:<10.3f}{:<8.0}"
        "{:<12.3f}{:<10.3f}{:<8.0}"
        "{:<12.3f}{:<10.3f}{:<8.0}".format(
            "Total",
            sum_df.LF.core_hours,
            sum_df.LF.run_time,
            "",
            sum_df.HF.core_hours,
            sum_df.HF.run_time,
            "",
            sum_df.BB.core_hours,
            sum_df.BB.run_time,
            "",
        )
    )


def get_runs_dir_params(
    runs_dir: str, fault_names: List[str], realisations: np.ndarray
):
    """Gets the parameters from the runs directory. Assumes that all realisations of a
    fault have the same parameters
    """
    data = []
    for ix, fault_name in enumerate(fault_names):
        r = realisations[ix][0]

        params = utils.load_sim_params(
            os.path.join(runs_dir, fault_name, r, "sim_params.yaml")
        )

        data.append((params.dt, params.hf.hf_dt, params.FD_STATLIST, params.hf.hf_slip))

    return np.rec.array(
        data,
        dtype=[
            ("dt", np.float32),
            ("hf_dt", np.float32),
            ("fd_statlist", np.object),
            ("hf_slip", np.object),
        ],
    )


def main(args):
    fault_names, realisations = get_faults(
        args.vms_dir, args.sources_dir, args.runs_dir, args
    )

    print("Collecting vm params")
    vm_params = (
        np.concatenate(
            [
                get_vm_params(fault_vm_path=os.path.join(args.vms_dir, fault))
                for fault in fault_names
            ]
        )
        .reshape(fault_names.shape[0], 5)
        .astype(np.float32)
    )

    config_dt, config_hf_dt = None, None
    if args.cybershake_config:
        print("Loading df and hf_dt from cybershake config")
        with open(args.cybershake_config, "r") as f:
            cybershake_config = json.load(f)

        config_dt = cybershake_config.get("dt")
        config_hf_dt = cybershake_config.get("hf_dt")

    # Get the params from the Runs directory
    # These are on a per fault basis, i.e. assuming that all realisations
    # of a fault have the same parameters!
    runs_params = (
        None
        if args.runs_dir is None
        else get_runs_dir_params(args.runs_dir, fault_names, realisations)
    )

    # Set dt
    if runs_params is not None:
        dt = runs_params.dt
        if config_dt is not None and np.any(dt != config_dt):
            print(
                "One of the fault dt's does not match the config dt. "
                "Something went wrong during install."
            )
    else:
        dt = (
            vm_params[:, 4]
            if config_dt is None
            else np.ones(fault_names.shape[0]) * config_dt
        )

    nan_mask = np.any(np.isnan(vm_params[:, :4]), axis=1) | np.isnan(dt)
    if np.any(nan_mask):
        print(
            "\nFollowing faults are missing vm values "
            "and are therefore dropped from the estimation: \n{}\n".format(
                "\n".join(fault_names[nan_mask])
            )
        )

        if fault_names[~nan_mask].shape[0] == 0:
            print("This leaves no faults. Quitting!")
            sys.exit()

        fault_names, realisations = fault_names[~nan_mask], realisations[~nan_mask]
        vm_params, dt = vm_params[~nan_mask], dt[~nan_mask]

    print("Preparing LF input data")
    fault_sim_durations = vm_params[:, 3]
    nt = fault_sim_durations / dt

    lf_ncores = (
        np.ones(fault_names.shape[0], dtype=np.float32) * estimate_wct.LF_DEFAULT_NCORES
    )
    lf_input_data = np.concatenate(
        (vm_params[:, :3], nt.reshape(-1, 1), lf_ncores.reshape(-1, 1)), axis=1
    )

    r_counts = [len(cur_r_list) for cur_r_list in realisations]

    # Estimate HF/BB if a runs directory is specified
    if args.runs_dir is not None:
        print("Preparing HF estimation input data")
        hf_ncores = (
            np.ones(realisations.shape[0], dtype=np.float32)
            * estimate_wct.HF_DEFAULT_NCORES
        )

        # Get fd_count and nsub_stoch for each fault
        fd_counts = np.asarray(
            [
                len(shared.get_stations(fd_statlist))
                for fd_statlist in runs_params.fd_statlist
            ]
        )

        nsub_stochs = np.asarray(
            [
                srf.get_nsub_stoch(hf_slip, get_area=False)
                for hf_slip in runs_params.hf_slip
            ]
        )

        # Have to repeat/extend the fault sim_durations to per realisation
        r_sim_durations = np.repeat(fault_sim_durations, r_counts)
        r_fd_counts = np.repeat(fd_counts, r_counts)
        r_nsub_stochs = np.repeat(nsub_stochs, r_counts)
        r_hf_nt = r_sim_durations / runs_params.hf_dt

        hf_input_data = np.concatenate(
            (
                r_fd_counts[:, None],
                r_nsub_stochs[:, None],
                r_hf_nt[:, None],
                hf_ncores[:, None],
            ),
            axis=1,
        )

        print("Preparing BB estimation input data")
        bb_ncores = (
            np.ones(realisations.shape[0], dtype=np.float32)
            * estimate_wct.BB_DEFAULT_NCORES
        )

        bb_input_data = np.concatenate(
            (fd_counts[:, None], r_hf_nt[:, None], bb_ncores[:, None]), axis=1
        )

        results_df = run_estimations(
            fault_names,
            realisations,
            r_counts,
            lf_input_data,
            hf_input_data,
            bb_input_data,
        )
    else:
        results_df = run_estimations(fault_names, realisations, r_counts, lf_input_data)

    results_df["fault_name"] = fault_names
    return results_df


if __name__ == "__main__":
    parser = ArgumentParser()

    parser.add_argument(
        "vms_dir", type=str, help="The absolute path to the VMs directory."
    )
    parser.add_argument(
        "sources_dir", type=str, help="The absolute path to the Sources directory."
    )
    parser.add_argument(
        "--runs_dir",
        type=str,
        help="The absolute path to the Runs directory."
        "Specifying this allows estimation of HF and BB.",
    )
    parser.add_argument(
        "--fault_selection",
        type=str,
        help="The cybershake fault selection text file."
        "Ignored if --runs_dir is specified.",
    )
    parser.add_argument(
        "--cybershake_config",
        type=str,
        help="Cybershake config file to retrieve dt."
        "Ignored if --runs_dir is specified.",
    )
    parser.add_argument(
        "-output",
        type=str,
        default=None,
        help="Where to save the resulting dataframe. Only saved if a "
        "valid file path is provided.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        default=False,
        help="Print estimation on a per fault basis instead of just "
        "the final estimation.",
    )

    args = parser.parse_args()

    # Check that the folders exists
    if not os.path.isdir(args.vms_dir):
        print("{} does not exists. Quitting!".format(args.vms_dir))
        sys.exit()

    if not os.path.isdir(args.sources_dir):
        print("{} does not exists. Quitting!".format(args.sources_dir))
        sys.exit()

    if args.runs_dir is not None and not os.path.isdir(args.runs_dir):
        print("{} does not exists. Quitting!".format(args.runs_dir))
        sys.exit()

    # Check that the specified files exist
    if args.cybershake_config is not None and not os.path.isfile(
        args.cybershake_config
    ):
        print("File {} does not exist".format(args.cybershake_config))
        sys.exit()

    if args.fault_selection is not None and not os.path.isfile(args.fault_selection):
        print("File {} does not exist".format(args.fault_selection))
        sys.exit()

    results_df = main(args)

    # Save the results
    if args.output is not None:
        if not os.path.isdir(os.path.dirname(args.output)):
            print("The provided file path is not valid. Output file not saved.")
        else:
            results_df.to_csv(args.output)

    display_results(results_df, True)
