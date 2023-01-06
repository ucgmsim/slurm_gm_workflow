#!/usr/bin/env python3
"""Script to create and submit a slurm script for HF"""
import argparse
from logging import Logger
from pathlib import Path

import workflow.automation.estimation.estimate_wct as est
from qcore import utils, shared, srf, binary_version
from qcore.config import host, get_machine_config
import qcore.constants as const
from qcore.qclogging import get_basic_logger
import qcore.simulation_structure as sim_struct
from workflow.automation.install_scripts.install_shared import HF_VEL_MOD_1D
from workflow.automation.lib.schedulers.scheduler_factory import Scheduler
from workflow.automation.platform_config import (
    platform_config,
    get_platform_node_requirements,
)

from workflow.automation.lib.shared import set_wct, get_hf_nt
from workflow.automation.lib.shared_automated_workflow import submit_script_to_scheduler
from workflow.automation.lib.shared_template import write_sl_script

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
        "duration": params.get("sim_duration", 0),
        "dt": params.hf.dt,
        "version": params.hf.version,
        "sim_bin_path": binary_version.get_hf_binmod(
            params.hf.version, get_machine_config(machine)["tools_dir"]
        ),
    }
    add_args = {}
    for k, v in params.hf.items():
        if v is False or v is None:
            continue
        add_args[k] = " ".join(map(str, v)) if (type(v) is list) else v

    add_args.update({const.RootParams.seed.value: seed})

    return command_template_parameters, add_args


def main(
    submit: bool = False,
    machine: str = host,
    ncores: int = platform_config[const.PLATFORM_CONFIG.HF_DEFAULT_NCORES.name],
    rel_dir: str = ".",
    retries: int = 0,
    seed: int = const.HF_DEFAULT_SEED,
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

    sim_dir = Path(params.sim_dir).resolve()

    if version in ["mpi", "run_hf_mpi"]:
        ll_name_prefix = "run_hf_mpi"
    else:
        version_default = platform_config[const.PLATFORM_CONFIG.HF_DEFAULT_VERSION.name]
        if version is None:
            logger.debug(f"version is not specified. Set to default: {version_default}")
        else:
            logger.error(
                f"{version} cannot be recognize as a valid version option. version is set to default: {version_default}"
            )
        version = version_default
        ll_name_prefix = version_default
    logger.debug(f"version: {version}")

    srf_name = Path(params.srf_file).stem

    nt = get_hf_nt(params)
    fd_count = len(shared.get_stations(params.FD_STATLIST))
    # TODO:make it read through the whole list
    #  instead of assuming every stoch has same size
    nsub_stoch, sub_fault_area = srf.get_nsub_stoch(params.hf.slip, get_area=True)

    est_core_hours, est_run_time, est_cores = est.est_HF_chours_single(
        fd_count, nsub_stoch, nt, ncores, scale_ncores=SCALE_NCORES, logger=logger
    )

    # scale up the est_run_time if it is a re-run (with check-pointing)
    # creates and extra variable so we keep the orignial estimated run time for other purpose
    est_run_time_scaled = est_run_time
    if retries > 0:
        # check if HF.bin is read-able = restart-able
        try:
            from qcore.timeseries import HFSeis

            bin = HFSeis(sim_struct.get_hf_bin_path(sim_dir))
        except:
            logger.debug("Retried count > 0 but HF.bin is not readable")
        else:
            est_run_time_scaled = est_run_time * (retries + 1)

    wct = set_wct(est_run_time_scaled, est_cores, submit)
    hf_sim_dir = sim_struct.get_hf_dir(sim_dir)
    if write_directory is None:
        write_directory = sim_dir

    underscored_srf = srf_name.replace("/", "__")

    header_dict = {
        "platform_specific_args": get_platform_node_requirements(est_cores),
        "wallclock_limit": wct,
        "job_name": f"hf.{underscored_srf}",
        "job_description": "HF calculation",
        "additional_lines": "",
    }
    command_template_parameters, add_args = gen_command_template(
        params, machine, seed=seed
    )
    assert command_template_parameters["duration"] != 0

    body_template_params = (
        f"{ll_name_prefix}.sl.template",
        {"hf_sim_dir": hf_sim_dir, "test_hf_script": "test_hf.sh"},
    )

    script_prefix = f"{ll_name_prefix}_{underscored_srf}"
    script_file_path = write_sl_script(
        write_directory,
        sim_dir,
        const.ProcessType.HF,
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
            const.ProcessType.HF.value,
            sim_struct.get_mgmt_db_queue(params.mgmt_db_location),
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
        description="Create (and submit if specified) the slurm script for HF"
    )

    parser.add_argument(
        "--account",
        type=str,
        default=platform_config[const.PLATFORM_CONFIG.DEFAULT_ACCOUNT.name],
    )

    # if the --submit flag is used, wall clock time will be estimated the job
    # submitted automatically
    parser.add_argument("--submit", action="store_true", default=False)

    parser.add_argument(
        "--machine",
        type=str,
        default=host,
        help="The machine hf is to be submitted to.",
    )
    parser.add_argument(
        "--ncores",
        type=int,
        default=platform_config[const.PLATFORM_CONFIG.HF_DEFAULT_NCORES.name],
    )

    parser.add_argument(
        "--rel_dir", default=".", type=str, help="The path to the realisation directory"
    )
    parser.add_argument(
        "--retries", default=0, type=int, help="Number of retries if fails"
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=const.HF_DEFAULT_SEED,
        help="random seed number(0 for randomized seed)",
    )
    # Uncomment when site_specific is implemented.

    # parser.add_argument(
    #     "--site_specific", action="store_true", default=False
    # )

    parser.add_argument("--version", type=str, default=None, const=None)

    parser.add_argument(
        "--write_directory",
        type=str,
        help="The directory to write the slurm script to.",
        default=None,
    )

    args = parser.parse_args()
    args.rel_dir = Path(args.rel_dir)

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
        args.seed,
        args.version,
        args.write_directory,
    )
