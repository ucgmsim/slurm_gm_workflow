#!/usr/bin/env python3
"""Script to create and submit a slurm script for HF"""
import os
import argparse

import estimation.estimate_wct as est
import qcore.constants as const
import qcore.simulation_structure as sim_struct
from qcore import utils, shared, srf, binary_version
from qcore.config import host, get_machine_config
from shared_workflow.load_config import load
from shared_workflow.shared import (
    set_wct,
    confirm,
    submit_sl_script,
    get_nt,
)
from shared_workflow.shared_template import write_sl_script

# default values
# Scale the number of nodes to be used for the simulation component
SCALE_NCORES = True
default_wct = "00:30:00"


def main(args: argparse.Namespace, est_model: est.EstModel = None):
    params = utils.load_sim_params(os.path.join(args.rel_dir, "sim_params.yaml"))

    # check if the args is none, if not, change the version

    if args.version is not None and args.version in ["mpi", "run_hf_mpi"]:
        version = args.version
        ll_name_prefix = "run_hf_mpi"
    else:
        if args.version is not None:
            print("{} cannot be recognize as a valid option".format(args.version))
            print("version is set to default: {}".format(const.HF_DEFAULT_VERSION))
        version = const.HF_DEFAULT_VERSION
        ll_name_prefix = const.HF_DEFAULT_VERSION
    print("version:", version)

    # modify the logic to use the same as in install_bb:
    # sniff through params_base to get the names of srf,
    # instead of running through file directories.

    # loop through all srf file to generate related slurm scripts
    srf_name = os.path.splitext(os.path.basename(params.srf_file))[0]
    # if srf(variation) is provided as args, only create
    # the slurm with same name provided
    if args.srf is None or srf_name == args.srf:
        nt = get_nt(params)
        fd_count = len(shared.get_stations(params.FD_STATLIST))
        # TODO:make it read through the whole list
        #  instead of assuming every stoch has same size
        nsub_stoch, sub_fault_area = srf.get_nsub_stoch(params.hf.slip, get_area=True)

        if est_model is None:
            workflow_config = load(
                os.path.dirname(os.path.realpath(__file__)), "workflow_config.json"
            )
            est_model = os.path.join(workflow_config["estimation_models_dir"], "HF")
        est_core_hours, est_run_time, est_cores = est.est_HF_chours_single(
            fd_count,
            nsub_stoch,
            nt,
            args.ncore,
            est_model,
            scale_ncores=SCALE_NCORES,
        )
        wct = set_wct(est_run_time, est_cores, args.auto)
        hf_sim_dir = os.path.join(params.sim_dir, "HF")
        write_directory = (
            args.write_directory if args.write_directory else params.sim_dir
        )
        underscored_srf = srf_name.replace("/", "__")

        header_dict = {
            "n_tasks": est_cores,
            "wallclock_limit": wct,
            "job_name": "sim_hf.{}".format(underscored_srf),
            "job_description": "HF calculation",
            "additional_lines": "###SBATCH -C avx",
        }

        command_template_parameters = {
            "fd_statlist": params.FD_STATLIST,
            "hf_bin_path": os.path.join(hf_sim_dir, "Acc/HF.bin"),
            "v_mod_1d_name": params.v_mod_1d_name,
            "duration": params.sim_duration,
            "dt": params.hf.dt,
            "version": params.hf.version,
            "sim_bin_path": binary_version.get_hf_binmod(
                params.hf.version, get_machine_config(args.machine)["tools_dir"]
            ),
        }

        body_template_params = (
            "{}.sl.template".format(ll_name_prefix),
            {"hf_sim_dir": hf_sim_dir, "test_hf_script": "test_hf_binary.sh"},
        )

        add_args = dict(params.hf)
        add_args.update({const.RootParams.seed.value: args.seed})

        script_prefix = "{}_{}".format(ll_name_prefix, underscored_srf)
        script_file_path = write_sl_script(
            write_directory,
            params.sim_dir,
            const.ProcessType.HF,
            script_prefix,
            header_dict,
            body_template_params,
            command_template_parameters,
            args,
            add_args,
        )

        # Submit the script
        submit_yes = True if args.auto else confirm("Also submit the job for you?")
        submit_sl_script(
            script_file_path,
            const.ProcessType.HF.value,
            sim_struct.get_mgmt_db_queue(params.mgmt_db_location),
            srf_name,
            submit_yes=submit_yes,
            target_machine=args.machine,
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Create (and submit if specified) the slurm script for HF"
    )

    parser.add_argument("--version", type=str, default=None, const=None)
    parser.add_argument("--ncore", type=int, default=const.HF_DEFAULT_NCORES)

    # if the --auto flag is used, wall clock time will be estimated the job
    # submitted automatically
    parser.add_argument("--auto", type=int, nargs="?", default=None, const=True)

    parser.add_argument(
        "--site_specific", type=int, nargs="?", default=None, const=True
    )
    parser.add_argument("--account", type=str, default=const.DEFAULT_ACCOUNT)
    parser.add_argument("--srf", type=str, default=None)
    parser.add_argument(
        "--seed",
        type=int,
        default=0,
        help="random seed number(0 for randomized seed)",
    )
    parser.add_argument(
        "--machine",
        type=str,
        default=host,
        help="The machine hf is to be submitted to.",
    )
    parser.add_argument(
        "--write_directory",
        type=str,
        help="The directory to write the slurm script to.",
        default=None,
    )
    parser.add_argument(
        "--rel_dir", default=".", type=str, help="The path to the realisation directory"
    )
    args = parser.parse_args()

    main(args)
