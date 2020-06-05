#!/usr/bin/env python3
"""Script to create and submit a slurm script for LF"""
import os
import glob
import argparse
from logging import Logger

from qcore import utils, binary_version
from qcore.config import get_machine_config, host, platform_config
import qcore.constants as const
from qcore.qclogging import get_basic_logger
import qcore.simulation_structure as sim_struct

from shared_workflow.shared import confirm
from shared_workflow.shared_automated_workflow import submit_sl_script
from shared_workflow.shared_template import write_sl_script


merge_ts_name_prefix = "post_emod3d_merge_ts"

# TODO: implement estimation for these numbers
default_run_time_merge_ts = "00:30:00"


def get_seis_len(seis_path):
    filepattern = os.path.join(seis_path, "*_seis*.e3d")
    seis_file_list = sorted(glob.glob(filepattern))
    return len(seis_file_list)


def main(args, logger: Logger = get_basic_logger()):
    params = utils.load_sim_params(os.path.join(args.rel_dir, "sim_params.yaml"))
    sim_dir = params.sim_dir
    mgmt_db_loc = params.mgmt_db_location
    submit_yes = True if args.auto else confirm("Also submit the job for you?")

    # get the srf(rup) name without extensions
    srf_name = os.path.splitext(os.path.basename(params.srf_file))[0]
    # if srf(variation) is provided as args, only create the slurm
    # with same name provided
    if args.srf is not None and srf_name != args.srf:
        return

    write_directory = args.write_directory if args.write_directory else sim_dir

    # get lf_sim_dir
    lf_sim_dir = os.path.join(sim_dir, "LF")

    header_dict = {
        "n_tasks": platform_config[const.PLATFORM_CONFIG.MERGE_TS_DEFAULT_NCORES.name],
        "wallclock_limit": default_run_time_merge_ts,
        "job_name": "post_emod3d.merge_ts.{}".format(srf_name),
        "job_description": "post emod3d: merge_ts",
        "additional_lines": "###SBATCH -C avx",
    }

    command_template_parameters = {
        "merge_ts_path": binary_version.get_unversioned_bin(
            "merge_tsP3_par", get_machine_config(args.machine)["tools_dir"]
        )
    }

    body_template_params = (
        "{}.sl.template".format(merge_ts_name_prefix),
        {"lf_sim_dir": lf_sim_dir},
    )

    script_prefix = "{}_{}".format(merge_ts_name_prefix, srf_name)
    script_file_path = write_sl_script(
        write_directory,
        sim_dir,
        const.ProcessType.merge_ts,
        script_prefix,
        header_dict,
        body_template_params,
        command_template_parameters,
        args,
    )
    if submit_yes:
        submit_sl_script(
            script_file_path,
            const.ProcessType.merge_ts.value,
            sim_struct.get_mgmt_db_queue(mgmt_db_loc),
            srf_name,
            target_machine=args.machine,
            logger=logger,
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Create (and submit if specified) the slurm script for HF"
    )
    parser.add_argument("--auto", nargs="?", type=str, const=True)
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
        help="The machine post_emod3d is to be submitted to.",
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
