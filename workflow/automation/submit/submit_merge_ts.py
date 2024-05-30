#!/usr/bin/env python3
"""Script to create and submit a slurm script for LF"""
import argparse
import glob
import os
from logging import Logger
from pathlib import Path

import qcore.constants as const
import qcore.simulation_structure as sim_struct
from qcore import utils
from qcore.config import host
from qcore.qclogging import get_basic_logger

from workflow.automation.lib.schedulers.scheduler_factory import Scheduler
from workflow.automation.lib.shared_automated_workflow import \
    submit_script_to_scheduler
from workflow.automation.lib.shared_template import write_sl_script
from workflow.automation.platform_config import (
    get_platform_node_requirements, platform_config)

merge_ts_name_prefix = "merge_ts"

default_run_time_merge_ts = "00:30:00"


def get_seis_len(seis_path):
    filepattern = os.path.join(seis_path, "*_seis*.e3d")
    seis_file_list = sorted(glob.glob(filepattern))
    return len(seis_file_list)


def main(
    submit: bool = False,
    machine: str = host,
    rel_dir: str = ".",
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

    mgmt_db_loc = params["mgmt_db_location"]

    # get the srf(rup) name without extensions
    srf_name = Path(params["srf_file"]).stem

    if write_directory is None:
        write_directory = sim_dir

    # get lf_sim_dir
    lf_sim_dir = sim_dir / "LF"

    header_dict = {
        "platform_specific_args": get_platform_node_requirements(
            platform_config[const.PLATFORM_CONFIG.MERGE_TS_DEFAULT_NCORES.name]
        ),
        "wallclock_limit": default_run_time_merge_ts,
        "job_name": "merge_ts.{}".format(srf_name),
        "job_description": "merge_ts",
        "additional_lines": "",
    }

    body_template_params = (
        "{}.sl.template".format(merge_ts_name_prefix),
        {"lf_sim_dir": lf_sim_dir},
    )
    command_template_parameters = {
        "run_command": platform_config[const.PLATFORM_CONFIG.RUN_COMMAND.name],
        "merge_ts_path": "merge_ts",
    }

    script_prefix = "{}_{}".format(merge_ts_name_prefix, srf_name)
    script_file_path = write_sl_script(
        write_directory,
        sim_dir,
        const.ProcessType.merge_ts,
        script_prefix,
        header_dict,
        body_template_params,
        command_template_parameters,
    )
    if submit:
        submit_script_to_scheduler(
            script_file_path,
            const.ProcessType.merge_ts.value,
            sim_struct.get_mgmt_db_queue(mgmt_db_loc),
            sim_dir,
            srf_name,
            target_machine=machine,
            logger=logger,
        )


def load_args():
    parser = argparse.ArgumentParser(
        description="Create (and submit if specified) the slurm script for merge_ts"
    )
    parser.add_argument("--submit", nargs="?", type=str, const=True)
    parser.add_argument(
        "--account",
        type=str,
        default=platform_config[const.PLATFORM_CONFIG.DEFAULT_ACCOUNT.name],
    )

    parser.add_argument(
        "--machine",
        type=str,
        default=host,
        help="The machine merge_ts is to be submitted to.",
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
    return args


if __name__ == "__main__":
    args = load_args()
    # The name parameter is only used to check user tasks in the queue monitor
    Scheduler.initialise_scheduler("", args.account)

    main(args.submit, args.machine, args.rel_dir, args.write_directory)
