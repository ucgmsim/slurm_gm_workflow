#!/usr/bin/env python3
"""Script to create and submit a slurm script for HF"""
import os
import argparse
from logging import Logger

import estimation.estimate_wct as est
from qcore import utils, shared, srf, binary_version
from qcore.config import host, get_machine_config
import qcore.constants as const
from qcore.qclogging import get_basic_logger
import qcore.simulation_structure as sim_struct
from shared_workflow.install_shared import HF_VEL_MOD_1D
from scripts.schedulers.scheduler_factory import Scheduler
from shared_workflow.platform_config import (
    platform_config,
    get_platform_node_requirements,
)

from shared_workflow.shared import set_wct, confirm, get_hf_nt
from shared_workflow.shared_automated_workflow import submit_script_to_scheduler
from shared_workflow.shared_template import write_sl_script

# default values
# Scale the number of nodes to be used for the simulation component

SCALE_NCORES = True
default_wct = "00:30:00"


def gen_command_template(params, machine, seed=const.HF_DEFAULT_SEED):
    command_template_parameters = {
        "run_command": platform_config[const.PLATFORM_CONFIG.RUN_COMMAND.name],
        "fd_statlist": params.FD_STATLIST,
        "hf_bin_path": sim_struct.get_hf_bin_path(params.sim_dir),
        HF_VEL_MOD_1D: params["hf"][HF_VEL_MOD_1D],
        "duration": params.sim_duration,
        "dt": params.hf.dt,
        "version": params.hf.version,
        "sim_bin_path": binary_version.get_hf_binmod(
            params.hf.version, get_machine_config(machine)["tools_dir"]
        ),
    }
    add_args = {}
    for k, v in params.hf.items():
        add_args[k] = " ".join(map(str, v)) if (type(v) is list) else v

    add_args.update({const.RootParams.seed.value: seed})

    return command_template_parameters, add_args


def main(
    args: argparse.Namespace,
    est_model: est.EstModel = None,
    logger: Logger = get_basic_logger(),
):
    params = utils.load_sim_params(os.path.join(args.rel_dir, "sim_params.yaml"))

    # check if the args is none, if not, change the version

    if args.version is not None and args.version in ["mpi", "run_hf_mpi"]:
        version = args.version
        ll_name_prefix = "run_hf_mpi"
    else:
        if args.version is not None:
            logger.error(
                "{} cannot be recognize as a valid version option. version is set to default: {}".format(
                    args.version,
                    platform_config[const.PLATFORM_CONFIG.HF_DEFAULT_VERSION.name],
                )
            )
        version = platform_config[const.PLATFORM_CONFIG.HF_DEFAULT_VERSION.name]
        ll_name_prefix = platform_config[const.PLATFORM_CONFIG.HF_DEFAULT_VERSION.name]
    logger.debug("version: {}".format(version))

    # modify the logic to use the same as in install_bb:
    # sniff through params_base to get the names of srf,
    # instead of running through file directories.

    # loop through all srf file to generate related slurm scripts
    srf_name = os.path.splitext(os.path.basename(params.srf_file))[0]
    # if srf(variation) is provided as args, only create
    # the slurm with same name provided
    if args.srf is None or srf_name == args.srf:
        nt = get_hf_nt(params)
        fd_count = len(shared.get_stations(params.FD_STATLIST))
        # TODO:make it read through the whole list
        #  instead of assuming every stoch has same size
        nsub_stoch, sub_fault_area = srf.get_nsub_stoch(params.hf.slip, get_area=True)

        if est_model is None:
            est_model = os.path.join(
                platform_config[const.PLATFORM_CONFIG.ESTIMATION_MODELS_DIR.name], "HF"
            )
        est_core_hours, est_run_time, est_cores = est.est_HF_chours_single(
            fd_count,
            nsub_stoch,
            nt,
            args.ncore,
            est_model,
            scale_ncores=SCALE_NCORES,
            logger=logger,
        )

        # scale up the est_run_time if it is a re-run (with check-pointing)
        # creates and extra variable so we keep the orignial estimated run time for other purpose
        est_run_time_scaled = est_run_time
        if hasattr(args, "retries") and int(args.retries) > 0:
            # check if HF.bin is read-able = restart-able
            try:
                from qcore.timeseries import HFSeis

                bin = HFSeis(sim_struct.get_hf_bin_path(params.sim_dir))
            except:
                logger.debug("Retried count > 0 but HF.bin is not readable")
            else:
                est_run_time_scaled = est_run_time * (int(args.retries) + 1)

        wct = set_wct(est_run_time_scaled, est_cores, args.auto)
        hf_sim_dir = sim_struct.get_hf_dir(params.sim_dir)
        write_directory = (
            args.write_directory if args.write_directory else params.sim_dir
        )
        underscored_srf = srf_name.replace("/", "__")

        header_dict = {
            "platform_specific_args": get_platform_node_requirements(est_cores),
            "wallclock_limit": wct,
            "job_name": "sim_hf.{}".format(underscored_srf),
            "job_description": "HF calculation",
            "additional_lines": "###SBATCH -C avx",
        }
        command_template_parameters, add_args = gen_command_template(
            params, args.machine, seed=args.seed
        )

        body_template_params = (
            "{}.sl.template".format(ll_name_prefix),
            {"hf_sim_dir": hf_sim_dir, "test_hf_script": "test_hf.sh"},
        )

        script_prefix = "{}_{}".format(ll_name_prefix, underscored_srf)
        script_file_path = write_sl_script(
            write_directory,
            params.sim_dir,
            const.ProcessType.HF,
            script_prefix,
            header_dict,
            body_template_params,
            command_template_parameters,
            add_args,
        )

        # Submit the script
        submit_yes = True if args.auto else confirm("Also submit the job for you?")
        if submit_yes:
            submit_script_to_scheduler(
                script_file_path,
                const.ProcessType.HF.value,
                sim_struct.get_mgmt_db_queue(params.mgmt_db_location),
                params.sim_dir,
                srf_name,
                target_machine=args.machine,
                logger=logger,
            )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Create (and submit if specified) the slurm script for HF"
    )

    parser.add_argument("--version", type=str, default=None, const=None)
    parser.add_argument(
        "--ncore",
        type=int,
        default=platform_config[const.PLATFORM_CONFIG.HF_DEFAULT_NCORES.name],
    )

    # if the --auto flag is used, wall clock time will be estimated the job
    # submitted automatically
    parser.add_argument("--auto", type=int, nargs="?", default=None, const=True)

    parser.add_argument(
        "--site_specific", type=int, nargs="?", default=None, const=True
    )
    parser.add_argument(
        "--account",
        type=str,
        default=platform_config[const.PLATFORM_CONFIG.DEFAULT_ACCOUNT.name],
    )
    parser.add_argument("--srf", type=str, default=None)
    parser.add_argument(
        "--seed",
        type=int,
        default=const.HF_DEFAULT_SEED,
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

    # The name parameter is only used to check user tasks in the queue monitor
    Scheduler.initialise_scheduler("", args.account)

    main(args)
