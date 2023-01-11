#!/usr/bin/env python3
"""Script to create and submit a slurm script for BB"""

import argparse
from logging import Logger
from pathlib import Path

import workflow.automation.estimation.estimate_wct as est
import qcore.constants as const
from qcore import simulation_structure
from qcore import utils, shared
from qcore.config import host
from qcore.qclogging import get_basic_logger
from workflow.automation.platform_config import (
    platform_config,
    get_platform_node_requirements,
)
from workflow.automation.lib.shared import set_wct, get_hf_nt
from workflow.automation.lib.shared_automated_workflow import submit_script_to_scheduler
from workflow.automation.lib.shared_template import write_sl_script
from workflow.automation.lib.schedulers.scheduler_factory import Scheduler

default_wct = "00:30:00"


def gen_command_template(params):
    command_template_parameters = {
        "run_command": platform_config[const.PLATFORM_CONFIG.RUN_COMMAND.name],
        "outbin_dir": simulation_structure.get_lf_outbin_dir(params["sim_dir"]),
        "vel_mod_dir": params["vel_mod_dir"],
        "hf_bin_path": simulation_structure.get_hf_bin_path(params["sim_dir"]),
        "stat_vs_est": params["stat_vs_est"],
        "bb_bin_path": simulation_structure.get_bb_bin_path(params["sim_dir"]),
        "flo": params["flo"],
    }

    return command_template_parameters, params["bb"]


def main(
    submit: bool = False,
    machine: str = host,
    ncores: int = platform_config[const.PLATFORM_CONFIG.BB_DEFAULT_NCORES.name],
    rel_dir: str = ".",
    retries: int = 0,
    version: str = None,
    write_directory: str = None,
    logger: Logger = get_basic_logger(),
):
    rel_dir = Path(rel_dir).resolve()
    try:
        params = utils.load_sim_params(rel_dir / "sim_params.yaml")
    except FileNotFoundError:
        logger.error(f"Error: sim_params.yaml doesn't exist in {rel_dir}")
        raise

    sim_dir = Path(params["sim_dir"]).resolve()

    if version in ["mpi", "run_bb_mpi"]:
        sl_name_prefix = "run_bb_mpi"
    else:
        version_default = platform_config[const.PLATFORM_CONFIG.BB_DEFAULT_VERSION.name]
        if version is None:
            logger.debug(f"version is not specified. Set to default: {version_default}")
        else:
            logger.error(
                f"{version} cannot be recognized as a valid option. version is set to default: {version_default}"
            )
        version = version_default
        sl_name_prefix = version_default
    logger.debug(version)

    srf_name = Path(params["srf_file"]).stem

    # TODO: save status as HF. refer to submit_hf
    # Use HF nt for wct estimation
    nt = get_hf_nt(params)
    fd_count = len(shared.get_stations(params["FD_STATLIST"]))

    est_core_hours, est_run_time = est.est_BB_chours_single(fd_count, nt, ncores)

    # creates and extra variable so we keep the original estimated run time for other purpos
    est_run_time_scaled = est_run_time
    if retries > 0:
        # check if BB.bin is read-able = restart-able
        try:
            from qcore.timeseries import BBSeis

            bin = BBSeis(simulation_structure.get_bb_bin_path(sim_dir))
        except:
            logger.debug("Retried count > 0 but BB.bin is not readable")
        else:
            est_run_time_scaled = est_run_time * (retries + 1)

    wct = set_wct(est_run_time_scaled, ncores, submit)

    if write_directory is None:
        write_directory = sim_dir

    underscored_srf = srf_name.replace("/", "__")

    header_dict = {
        "wallclock_limit": wct,
        "job_name": f"bb.{underscored_srf}",
        "job_description": "BB calculation",
        "additional_lines": "",
        "platform_specific_args": get_platform_node_requirements(ncores),
    }

    body_template_params = (
        "{}.sl.template".format(sl_name_prefix),
        {"test_bb_script": "test_bb.sh"},
    )

    command_template_parameters, add_args = gen_command_template(params)

    script_prefix = f"{sl_name_prefix}_{underscored_srf}"
    script_file_path = write_sl_script(
        write_directory,
        sim_dir,
        const.ProcessType.BB,
        script_prefix,
        header_dict,
        body_template_params,
        command_template_parameters,
        add_args,
    )

    # Submit the script
    if submit:
        submit_script_to_scheduler(
            script_file_path,
            const.ProcessType.BB.value,
            simulation_structure.get_mgmt_db_queue(params["mgmt_db_location"]),
            sim_dir,
            srf_name,
            target_machine=machine,
            logger=logger,
        )


def load_args():
    """
    Unpacks arguments and does basic checks

    Returns
    -------
    Processed arguments

    """
    parser = argparse.ArgumentParser(
        description="Create (and submit if specified) the slurm script for BB"
    )

    parser.add_argument("--submit", action="store_true", default=False)

    parser.add_argument(
        "--account",
        type=str,
        default=platform_config[const.PLATFORM_CONFIG.DEFAULT_ACCOUNT.name],
    )

    parser.add_argument(
        "--machine",
        type=str,
        default=host,
        help="The machine bb is to be submitted to.",
    )

    parser.add_argument(
        "--ncores",
        type=int,
        default=platform_config[const.PLATFORM_CONFIG.BB_DEFAULT_NCORES.name],
    )

    parser.add_argument(
        "--rel_dir", default=".", type=str, help="The path to the realisation directory"
    )
    parser.add_argument(
        "--retries", default=0, type=int, help="Number of retries if fails"
    )
    parser.add_argument(
        "--version",
        type=str,
        default=platform_config[const.PLATFORM_CONFIG.BB_DEFAULT_VERSION.name],
    )
    parser.add_argument(
        "--write_directory",
        type=str,
        help="The directory to write the slurm script to.",
        default=None,
    )

    args = parser.parse_args()
    return args


if __name__ == "__main__":

    args = load_args()
    # The name parameter is only used to check user tasks in the queue monitor
    Scheduler.initialise_scheduler("", args.account)

    main(
        args.submit,
        args.machine,
        args.ncores,
        args.rel_dir,
        args.retries,
        args.srf,
        args.version,
        args.write_directory,
    )
