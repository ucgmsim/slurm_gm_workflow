#!/usr/bin/env python3
"""Script to create and submit a slurm script for BB"""
import os
import argparse
from logging import Logger

import estimation.estimate_wct as est
import qcore.constants as const
from qcore import simulation_structure
from qcore import utils, shared
from qcore.config import host
from qcore.qclogging import get_basic_logger
from shared_workflow.platform_config import platform_config
from shared_workflow.shared import set_wct, confirm, get_hf_nt
from shared_workflow.shared_automated_workflow import submit_sl_script
from shared_workflow.shared_template import write_sl_script

default_wct = "00:30:00"


def gen_command_template(params):
    command_template_parameters = {
        "run_command": platform_config[const.PLATFORM_CONFIG.RUN_COMMAND.name],
        "outbin_dir": simulation_structure.get_lf_outbin_dir(params.sim_dir),
        "vel_mod_dir": params.vel_mod_dir,
        "hf_bin_path": simulation_structure.get_hf_bin_path(params.sim_dir),
        "stat_vs_est": params.stat_vs_est,
        "bb_bin_path": simulation_structure.get_bb_bin_path(params.sim_dir),
        "flo": params.flo,
    }

    return command_template_parameters, params.bb


def main(
    args: argparse.Namespace,
    est_model: est.EstModel = None,
    logger: Logger = get_basic_logger(),
):
    params = utils.load_sim_params(os.path.join(args.rel_dir, "sim_params.yaml"))
    ncores = platform_config[const.PLATFORM_CONFIG.BB_DEFAULT_NCORES.name]

    version = args.version
    if version in ["mpi", "run_bb_mpi"]:
        sl_name_prefix = "run_bb_mpi"
    else:
        if version is not None:
            logger.error(
                "{} cannot be recognized as a valid option. version is set to default:".format(
                    version,
                    platform_config[const.PLATFORM_CONFIG.BB_DEFAULT_VERSION.name],
                )
            )
        version = platform_config[const.PLATFORM_CONFIG.BB_DEFAULT_VERSION.name]
        sl_name_prefix = platform_config[const.PLATFORM_CONFIG.BB_DEFAULT_VERSION.name]
    logger.debug(version)

    srf_name = os.path.splitext(os.path.basename(params.srf_file))[0]
    if args.srf is None or srf_name == args.srf:
        # TODO: save status as HF. refer to submit_hf
        # Use HF nt for wct estimation
        nt = get_hf_nt(params)
        fd_count = len(shared.get_stations(params.FD_STATLIST))

        if est_model is None:
            est_model = os.path.join(
                platform_config[const.PLATFORM_CONFIG.ESTIMATION_MODELS_DIR.name], "BB"
            )

        est_core_hours, est_run_time = est.est_BB_chours_single(
            fd_count, nt, ncores, est_model
        )

        # creates and extra variable so we keep the original estimated run time for other purpos
        est_run_time_scaled = est_run_time
        if hasattr(args, "retries") and int(args.retries) > 0:
            # check if BB.bin is read-able = restart-able
            try:
                from qcore.timeseries import BBSeis

                bin = BBSeis(simulation_structure.get_bb_bin_path(params.sim_dir))
            except:
                logger.debug("Retried count > 0 but BB.bin is not readable")
            else:
                est_run_time_scaled = est_run_time * (int(args.retries) + 1)

        wct = set_wct(est_run_time_scaled, ncores, args.auto)
        write_directory = (
            args.write_directory if args.write_directory else params.sim_dir
        )
        underscored_srf = srf_name.replace("/", "__")

        header_dict = {
            "n_tasks": ncores,
            "wallclock_limit": wct,
            "job_name": "sim_bb.{}".format(underscored_srf),
            "job_description": "BB calculation",
            "additional_lines": "###SBATCH -C avx",
        }

        body_template_params = (
            "{}.sl.template".format(sl_name_prefix),
            {"test_bb_script": "test_bb.sh"},
        )

        command_template_parameters, add_args = gen_command_template(params)

        script_prefix = "{}_{}".format(sl_name_prefix, underscored_srf)
        script_file_path = write_sl_script(
            write_directory,
            params.sim_dir,
            const.ProcessType.BB,
            script_prefix,
            header_dict,
            body_template_params,
            command_template_parameters,
            add_args,
        )

        # Submit the script
        submit_yes = True if args.auto else confirm("Also submit the job for you?")
        if submit_yes:
            submit_sl_script(
                script_file_path,
                const.ProcessType.BB.value,
                simulation_structure.get_mgmt_db_queue(params.mgmt_db_location),
                params.sim_dir,
                srf_name,
                target_machine=args.machine,
                logger=logger,
            )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Create (and submit if specified) the slurm script for BB"
    )

    parser.add_argument("--auto", nargs="?", type=str, const=True)
    parser.add_argument(
        "--version",
        type=str,
        default=platform_config[const.PLATFORM_CONFIG.BB_DEFAULT_VERSION.name],
    )
    parser.add_argument(
        "--account",
        type=str,
        default=platform_config[const.PLATFORM_CONFIG.DEFAULT_ACCOUNT.name],
    )
    parser.add_argument("--srf", type=str, default=None)
    parser.add_argument(
        "--machine",
        type=str,
        default=host,
        help="The machine bb is to be submitted to.",
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
