#!/usr/bin/env python3
"""Script to create and submit a slurm script for LF"""
import os
import glob
import argparse

import qcore.constants as const
import qcore.simulation_structure as sim_struct
from qcore import utils, binary_version
from qcore.config import get_machine_config, host
from shared_workflow.shared import (
    confirm,
    submit_sl_script,
    resolve_header,
    generate_context,
    write_sl_script,
)


merge_ts_name_prefix = "post_emod3d_merge_ts"
winbin_aio_name_prefix = "post_emod3d_winbin_aio"

# TODO: implement estimation for these numbers
default_run_time_merge_ts = "00:30:00"
default_run_time_winbin_aio = "02:00:00"

# default_core_merge_ts must be 4, higher number of cpu cause
# un-expected errors (TODO: maybe fix it when merg_ts's time become issue)
default_core_winbin_aio = "80"

# TODO:the max number of cpu per node may need update when migrate machines
# this variable is critical to prevent crashes for winbin-aio
max_tasks_per_node = "80"


def get_seis_len(seis_path):
    filepattern = os.path.join(seis_path, "*_seis*.e3d")
    seis_file_list = sorted(glob.glob(filepattern))
    return len(seis_file_list)


def main(args):
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

    # run merge_ts related scripts only
    # if non of args are provided, run script related to both
    if not args.winbin_aio and not args.merge_ts:
        args.merge_ts, args.winbin_aio = True, True

    if args.merge_ts:
        header_dict = {
            "n_tasks": const.MERGE_TS_DEFAULT_NCORES,
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

        submit_sl_script(
            script_file_path,
            const.ProcessType.merge_ts.value,
            sim_struct.get_mgmt_db_queue(mgmt_db_loc),
            srf_name,
            submit_yes=submit_yes,
            target_machine=args.machine,
        )

    # run winbin_aio related scripts only
    if args.winbin_aio:

        # Get the file count of seis files
        sfl_len = int(
            get_seis_len(os.path.join(os.path.join(sim_dir, lf_sim_dir), "OutBin"))
        )

        # Round down to the max cpu per node
        nodes = int(round((sfl_len / int(max_tasks_per_node)) - 0.5))
        if nodes <= 0:
            # Use the same cpu count as the seis files
            nb_cpus = str(sfl_len)
        else:
            nb_cpus = str(nodes * int(max_tasks_per_node))

        header_dict = {
            "n_tasks": nb_cpus,
            "wallclock_limit": default_run_time_winbin_aio,
            "job_name": "post_emod3d.winbin_aio.{}".format(srf_name),
            "job_description": "post emod3d: winbin_aio",
            "additional_lines": "###SBATCH -C avx",
        }

        command_template_parameters = {"lf_sim_dir": lf_sim_dir}

        body_template_params = ("{}.sl.template".format(winbin_aio_name_prefix), {})

        script_prefix = "{}_{}".format(winbin_aio_name_prefix, srf_name)
        script_file_path = write_sl_script(
            write_directory,
            sim_dir,
            const.ProcessType.winbin_aio,
            script_prefix,
            header_dict,
            body_template_params,
            command_template_parameters,
            args,
        )

        submit_sl_script(
            script_file_path,
            const.ProcessType.winbin_aio.value,
            sim_struct.get_mgmt_db_queue(params.mgmt_db_location),
            srf_name,
            submit_yes=submit_yes,
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Create (and submit if specified) the slurm script for HF"
    )

    parser.add_argument("--auto", nargs="?", type=str, const=True)
    parser.add_argument("--account", type=str, default=const.DEFAULT_ACCOUNT)
    parser.add_argument("--merge_ts", nargs="?", type=str, const=True)
    parser.add_argument("--winbin_aio", nargs="?", type=str, const=True)
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
