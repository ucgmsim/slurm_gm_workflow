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
from estimation import estimate_wct
from shared_workflow.load_config import load

VM_PARAMS_FILENAME = "vm_params.yaml"

# The node time threshold factor used for ncores scaling
NODE_TIME_TH_FACTOR = 0.5


def get_faults(vms_dir, sources_dir, runs_dir, args):
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
    model_dirs_dict,
    lf_input_data,
    hf_input_data=None,
    bb_input_data=None,
):
    """Runs the estimations for LF and BB, HF (if the runs_dir argument was set)
    and returns a dataframe containing the results for
    each fault/realisation combination.
    """
    print("Running estimation for LF")
    lf_core_hours, lf_run_time, lf_ncores = estimate_wct.estimate_LF_chours(
        lf_input_data, model_dirs_dict["LF"], True
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
            hf_input_data, model_dirs_dict["HF"], True
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
        bb_core_hours, bb_run_time = estimate_wct.estimate_BB_chours(
            bb_input_data, model_dirs_dict["BB"]
        )
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
            hf_str = "{:<12.3f}{:<10.3f}{:<8.0f}".format(
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
            bb_str = "{:<12.3f}{:<10.3f}{:<8.0f}".format(
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


def main(args):
    fault_names, realisations = get_faults(
        args.vms_dir, args.sources_dir, args.runs_dir, args
    )

    if args.models_dir is None:
        workflow_config = load(
            os.path.join(os.path.dirname(os.path.abspath(__file__)), "../", "scripts"),
            "workflow_config.json",
        )
        models_dir = workflow_config["estimation_models_dir"]
    else:
        models_dir = args.models_dir
    model_dir_dict = {
        "LF": os.path.join(models_dir, "LF"),
        "HF": os.path.join(models_dir, "HF"),
        "BB": os.path.join(models_dir, "BB"),
    }

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
    if args.root_yaml:
        print("Loading df and hf_dt from root_default.yaml")
        with open(args.root_yaml, "r") as f:
            root_config = yaml.load(f)

        config_dt = root_config.get("dt")

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
        np.ones(fault_names.shape[0], dtype=np.float32) * const.LF_DEFAULT_NCORES
    )
    lf_input_data = np.concatenate(
        (vm_params[:, :3], nt.reshape(-1, 1), lf_ncores.reshape(-1, 1)), axis=1
    )

    r_counts = [len(cur_r_list) for cur_r_list in realisations]

    # Estimate HF/BB if a runs directory is specified
    if args.runs_dir is not None:
        print("Preparing HF estimation input data")
        # Have to repeat/extend the fault sim_durations to per realisation
        r_hf_ncores = np.repeat(
            np.ones(realisations.shape[0], dtype=np.float32) * const.HF_DEFAULT_NCORES,
            r_counts,
        )

        # Get fd_count and nsub_stoch for each fault
        r_fd_counts = np.repeat(
            np.asarray(
                [
                    len(shared.get_stations(fd_statlist))
                    for fd_statlist in runs_params.fd_statlist
                ]
            ),
            r_counts,
        )

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
            np.ones(realisations.shape[0], dtype=np.float32) * const.BB_DEFAULT_NCORES,
            r_counts,
        )

        bb_input_data = np.concatenate(
            (r_fd_counts[:, None], r_hf_nt[:, None], r_bb_ncores[:, None]), axis=1
        )

        results_df = run_estimations(
            fault_names,
            realisations,
            r_counts,
            model_dir_dict,
            lf_input_data,
            hf_input_data,
            bb_input_data,
        )
    else:
        results_df = run_estimations(
            fault_names, realisations, r_counts, model_dir_dict, lf_input_data
        )

    results_df["fault_name"] = np.repeat(fault_names, r_counts)
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
        default=None,
        help="The absolute path to the Runs directory."
        "Specifying this allows estimation of HF and BB.",
    )
    parser.add_argument(
        "--fault_selection",
        type=str,
        default=None,
        help="The cybershake fault selection text file."
        "Ignored if --runs_dir is specified.",
    )
    parser.add_argument(
        "--root_yaml",
        type=str,
        default=None,
        help="root_default.yaml file to retrieve dt."
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
    parser.add_argument(
        "--models_dir",
        type=str,
        default=None,
        help="The models directory (i.e. ..../estimation/models/. If not specified"
        "then the model dir from the workflow config is used.",
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
    if args.root_yaml is not None and not os.path.isfile(args.root_yaml):
        print("File {} does not exist".format(args.root_yaml))
        sys.exit()

    if args.fault_selection is not None and not os.path.isfile(args.fault_selection):
        print("File {} does not exist".format(args.fault_selection))
        sys.exit()

    results_df = main(args)

    # Save the results
    results_df.to_csv(args.output)

    display_results(results_df, args.verbose)
