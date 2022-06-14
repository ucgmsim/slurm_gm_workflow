#!/usr/bin/env python3
"""Script for estimating core hours and run time for LF, HH, BB
for the specified srf/vm (and runs) folder
"""
import sys
import os
import pandas as pd
import numpy as np
import yaml

from argparse import ArgumentParser
from typing import List

import qcore.constants as const
from qcore import shared, srf, utils
from workflow.automation.lib.shared import get_stations
from workflow.automation.estimation import estimate_wct
from workflow.automation.platform_config import platform_config


VM_PARAMS_FILENAME = "vm_params.yaml"

# The node time threshold factor used for ncores scaling
NODE_TIME_TH_FACTOR = 0.5


def get_faults(vms_dir, sources_dir, runs_dir, fault_selection=None):
    """Gets the faults and their realisation counts.
    Handles the different cases from the specified user arguments.
    """

    def get_realisations(faults, path):
        """Gets the realisation for each of the specified faults.
        Assumes that the directories exists, no checking is done.
        """
        return np.asarray(
            [
                np.asarray(
                    [
                        entry
                        for entry in os.listdir(os.path.join(path, fault))
                        if os.path.isdir(os.path.join(path, fault, entry))
                        and fault in entry
                    ]
                )
                for fault in faults
            ]
        )

    faults_df = (
        None
        if fault_selection is None
        else pd.read_table(
            fault_selection,
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
    elif runs_dir is None and faults_df is not None:
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

    realisations = get_realisations(faults, runs_dir)

    print(
        "Found {} faults and {} realisations".format(
            faults.shape[0], np.sum([len(cur_r_list) for cur_r_list in realisations])
        )
    )

    return faults, realisations


def get_vm_params(fault_vm_path):
    """Gets nx, ny, nz and dt from the velocity params file"""
    vm_params_dict = utils.load_yaml(os.path.join(fault_vm_path, VM_PARAMS_FILENAME))

    return [
        vm_params_dict.get("nx", np.nan),
        vm_params_dict.get("ny", np.nan),
        vm_params_dict.get("nz", np.nan),
        vm_params_dict.get("sim_duration", np.nan),
        vm_params_dict.get("dt", np.nan),
    ]


def run_estimations(
    fault_names,
    realisations,
    r_counts,
    lf_input_data,
    hf_input_data=None,
    bb_input_data=None,
    im_calc_input_data=None,
):
    """Runs the estimations for LF and BB, HF, IM_calc (if the runs_dir argument was set)
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
        [
            (const.ProcessType.EMOD3D.str_value, data_col.value)
            for data_col in [
                const.MetadataField.core_hours,
                const.MetadataField.run_time,
                const.MetadataField.n_cores,
            ]
        ]
    )

    results_df = pd.DataFrame(lf_result_data, index=index, columns=lf_columns)

    if hf_input_data is not None:
        print("Running HF estimation")
        hf_core_hours, hf_run_time, hf_cores = estimate_wct.estimate_HF_chours(
            hf_input_data, True
        )
    else:
        hf_core_hours, hf_run_time, hf_cores = np.nan, np.nan, np.nan

    results_df[
        (const.ProcessType.HF.str_value, const.MetadataField.core_hours.value)
    ] = hf_core_hours
    results_df[
        (const.ProcessType.HF.str_value, const.MetadataField.run_time.value)
    ] = hf_run_time
    results_df[
        (const.ProcessType.HF.str_value, const.MetadataField.n_cores.value)
    ] = hf_cores

    if bb_input_data is not None:
        print("Running BB estimation")
        bb_core_hours, bb_run_time = estimate_wct.estimate_BB_chours(bb_input_data)
        bb_cores = bb_input_data[:, -1]
    else:
        bb_core_hours, bb_run_time, bb_cores = np.nan, np.nan, np.nan

    results_df[
        (const.ProcessType.BB.str_value, const.MetadataField.core_hours.value)
    ] = bb_core_hours
    results_df[
        (const.ProcessType.BB.str_value, const.MetadataField.run_time.value)
    ] = bb_run_time
    results_df[
        (const.ProcessType.BB.str_value, const.MetadataField.n_cores.value)
    ] = bb_cores

    if im_calc_input_data is not None:
        print("Running IM_calc estimation")
        im_calc_core_hours, im_calc_run_time = estimate_wct.est_IM_chours_single(
            *im_calc_input_data
        )
        im_calc_cores = [im_calc_input_data[-1]] * sum(r_counts)
    else:
        im_calc_core_hours, im_calc_run_time, im_calc_cores = np.nan, np.nan, np.nan

    results_df[
        (
            const.ProcessType.IM_calculation.str_value,
            const.MetadataField.core_hours.value,
        )
    ] = im_calc_core_hours
    results_df[
        (const.ProcessType.IM_calculation.str_value, const.MetadataField.run_time.value)
    ] = im_calc_run_time
    results_df[
        (const.ProcessType.IM_calculation.str_value, const.MetadataField.n_cores.value)
    ] = im_calc_cores

    return results_df


def main(
    root_dir: str,
    fault_selection: str = None,
):
    vms_dir = f"{root_dir}/Data/VMs"
    sources_dir = f"{root_dir}/Data/Sources"
    runs_dir = f"{root_dir}/Runs"
    fault_names, realisations = get_faults(
        vms_dir, sources_dir, runs_dir, fault_selection
    )

    print("Collecting vm params")
    vm_params = (
        np.concatenate(
            [
                get_vm_params(fault_vm_path=os.path.join(vms_dir, fault))
                for fault in fault_names
            ]
        )
        .reshape(fault_names.shape[0], 5)
        .astype(np.float32)
    )

    config_dt, config_hf_dt = None, None
    print("Loading df and hf_dt from root_params.yaml")
    root_config = utils.load_yaml(f"{runs_dir}/root_params.yaml")

    config_dt = root_config.get("dt")

    # Get the params from the Runs directory
    # These are on a per fault basis, i.e. assuming that all realisations
    # of a fault have the same parameters!
    runs_params = (
        None
        if runs_dir is None
        else get_runs_dir_params(runs_dir, fault_names, realisations)
    )

    # Set dt
    if runs_params is not None:
        dt = runs_params.dt
        if np.any(dt != config_dt):
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
        np.ones(fault_names.shape[0], dtype=np.float32)
        * platform_config[const.PLATFORM_CONFIG.LF_DEFAULT_NCORES.name]
    )

    # Get fd_count for each fault
    fd_counts = np.asarray(
        [
            len(shared.get_stations(fd_statlist))
            for fd_statlist in runs_params.fd_statlist
        ]
    )
    r_counts = [len(cur_r_list) for cur_r_list in realisations]
    lf_input_data = np.concatenate(
        (
            vm_params[:, :3],
            nt.reshape(-1, 1),
            fd_counts.reshape(-1, 1),
            lf_ncores.reshape(-1, 1),
        ),
        axis=1,
    )

    print("Preparing HF estimation input data")
    # Have to repeat/extend the fault sim_durations to per realisation
    r_hf_ncores = np.repeat(
        np.ones(realisations.shape[0], dtype=np.float32)
        * platform_config[const.PLATFORM_CONFIG.HF_DEFAULT_NCORES.name],
        r_counts,
    )

    # Get fd_count and nsub_stoch for each realization
    r_fd_counts = np.repeat(fd_counts, r_counts)
    r_nsub_stochs = np.repeat(
        np.asarray(
            [srf.get_nsub_stoch(slip, get_area=False) for slip in runs_params.slip]
        ),
        r_counts,
    )

    # Calculate nt
    r_hf_nt = np.repeat(fault_sim_durations / runs_params.hf_dt, r_counts)

    hf_input_data = np.concatenate(
        (
            r_fd_counts[:, None],
            r_nsub_stochs[:, None],
            r_hf_nt[:, None],
            r_hf_ncores[:, None],
        ),
        axis=1,
    )

    print("Preparing BB estimation input data")
    r_bb_ncores = np.repeat(
        np.ones(realisations.shape[0], dtype=np.float32)
        * platform_config[const.PLATFORM_CONFIG.BB_DEFAULT_NCORES.name],
        r_counts,
    )

    bb_input_data = np.concatenate(
        (r_fd_counts[:, None], r_hf_nt[:, None], r_bb_ncores[:, None]), axis=1
    )

    print("Preparing IM_calc input data")
    if root_config["ims"][const.RootParams.extended_period.name]:
        period_count = len(
            np.unique(np.append(root_config["ims"]["pSA_periods"], const.EXT_PERIOD))
        )
    else:
        period_count = len(root_config["ims"]["pSA_periods"])
    im_calc_input_data = [
        len(get_stations(root_config["stat_file"])),
        np.repeat(fault_sim_durations / float(root_config["dt"]), r_counts),
        root_config["ims"][const.SlBodyOptConsts.component.value],
        period_count,
        platform_config[const.PLATFORM_CONFIG.IM_CALC_DEFAULT_N_CORES.name],
    ]

    results_df = run_estimations(
        fault_names,
        realisations,
        r_counts,
        lf_input_data,
        hf_input_data,
        bb_input_data,
        im_calc_input_data,
    )

    results_df["fault_name"] = np.repeat(fault_names, r_counts)
    return results_df


def display_results(df: pd.DataFrame, verbose: bool = False):
    """Displays the results in a nice format.
    If verbose is specified the results are shown on a per fault basis.
    """
    print()
    process_types = "{:>14}{:>30}{:>30}{:>35}".format("LF", "HF", "BB", "IM_calc")
    if verbose:
        header = "{:<12}{:<10}{:<8}".format("core hours", "run time", "cores")
        print(process_types)
        print("{:>12}{}{}{}{}".format("", header, header, header, header))
        process_type_result = "{:<12.3f}{:<10.3f}{:<8.0f}"
        for fault_name, row in df.groupby("fault_name").sum().iterrows():
            lf_str = process_type_result.format(
                row.loc[
                    (
                        const.ProcessType.EMOD3D.str_value,
                        const.MetadataField.core_hours.value,
                    )
                ],
                row.loc[
                    (
                        const.ProcessType.EMOD3D.str_value,
                        const.MetadataField.run_time.value,
                    )
                ],
                row.loc[
                    (
                        const.ProcessType.EMOD3D.str_value,
                        const.MetadataField.n_cores.value,
                    )
                ],
            )
            hf_str = process_type_result.format(
                row.loc[
                    (
                        const.ProcessType.HF.str_value,
                        const.MetadataField.core_hours.value,
                    )
                ],
                row.loc[
                    (const.ProcessType.HF.str_value, const.MetadataField.run_time.value)
                ],
                row.loc[
                    (const.ProcessType.HF.str_value, const.MetadataField.n_cores.value)
                ],
            )
            bb_str = process_type_result.format(
                row.loc[
                    (
                        const.ProcessType.BB.str_value,
                        const.MetadataField.core_hours.value,
                    )
                ],
                row.loc[
                    (const.ProcessType.BB.str_value, const.MetadataField.run_time.value)
                ],
                row.loc[
                    (const.ProcessType.BB.str_value, const.MetadataField.n_cores.value)
                ],
            )
            im_calc_str = process_type_result.format(
                row.loc[
                    (
                        const.ProcessType.IM_calculation.str_value,
                        const.MetadataField.core_hours.value,
                    )
                ],
                row.loc[
                    (
                        const.ProcessType.IM_calculation.str_value,
                        const.MetadataField.run_time.value,
                    )
                ],
                row.loc[
                    (
                        const.ProcessType.IM_calculation.str_value,
                        const.MetadataField.n_cores.value,
                    )
                ],
            )
            print(
                "{:<12}{}{}{}{}".format(fault_name, lf_str, hf_str, bb_str, im_calc_str)
            )

    print()
    sum_df = df.sum()
    header = "{:<12}{:<10}{:<8}".format("core hours", "run time", "")
    print(process_types)
    print("{:>12}{}{}{}{}".format("", header, header, header, header))
    print(
        "{:<12}{:<12.3f}{:<10.3f}{:<8.0}"
        "{:<12.3f}{:<10.3f}{:<8.0}"
        "{:<12.3f}{:<10.3f}{:<8.0}"
        "{:<12.3f}{:<10.3f}{:<8.0}".format(
            "Total",
            sum_df.loc[
                const.ProcessType.EMOD3D.str_value, const.MetadataField.core_hours.value
            ],
            sum_df.loc[
                const.ProcessType.EMOD3D.str_value, const.MetadataField.run_time.value
            ],
            "",
            sum_df.loc[
                const.ProcessType.HF.str_value, const.MetadataField.core_hours.value
            ],
            sum_df.loc[
                const.ProcessType.HF.str_value, const.MetadataField.run_time.value
            ],
            "",
            sum_df.loc[
                const.ProcessType.BB.str_value, const.MetadataField.core_hours.value
            ],
            sum_df.loc[
                const.ProcessType.BB.str_value, const.MetadataField.run_time.value
            ],
            "",
            sum_df.loc[
                const.ProcessType.IM_calculation.str_value,
                const.MetadataField.core_hours.value,
            ],
            sum_df.loc[
                const.ProcessType.IM_calculation.str_value,
                const.MetadataField.run_time.value,
            ],
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

        data.append((params.dt, params.hf.dt, params.FD_STATLIST, params.hf.slip))

    return np.rec.array(
        data,
        dtype=[
            ("dt", np.float32),
            ("hf_dt", np.float32),
            ("fd_statlist", np.object),
            ("slip", np.object),
        ],
    )


if __name__ == "__main__":
    parser = ArgumentParser()

    parser.add_argument(
        "root_dir", type=str, help="The absolute path to the cybershake root directory."
    )
    parser.add_argument(
        "--fault_selection",
        type=str,
        default=None,
        help="The cybershake fault selection text file."
        "Ignored if --runs_dir is specified.",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Where to save the resulting dataframe. Only saved if a "
        "valid file path is provided.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print estimation on a per fault basis instead of just "
        "the final estimation.",
    )

    args = parser.parse_args()

    # Check that the folder exists
    if not os.path.isdir(args.root_dir):
        print("{} does not exists. Quitting!".format(args.vms_dir))
        sys.exit()

    # Check that the specified file exist
    if args.fault_selection is not None and not os.path.isfile(args.fault_selection):
        print("File {} does not exist".format(args.fault_selection))
        sys.exit()

    results_df = main(
        args.root_dir,
        args.fault_selection,
    )

    # Save the results
    results_df.to_csv(args.output)

    display_results(results_df, args.verbose)
