#!/usr/bin/env python3
"""Script to create and submit a slurm script for BB"""
import os
import argparse

import estimation.estimate_wct as est
import qcore.constants as const
from qcore import utils, shared
from qcore.config import host
from shared_workflow.shared import (
    set_wct,
    confirm,
    submit_sl_script,
    write_sl_script,
    get_nt)

BB_COMMAND_TEMPLATE = "srun python $gmsim/workflow/scripts/bb_sim.py {outbin_dir} {vel_mod_dir} {hf_bin_path} " \
                      "{stat_vs_est} {bb_bin_path} --flo {flo}"

default_wct = "00:30:00"


def main(args):
    params = utils.load_sim_params("sim_params.yaml")

    ncores = const.BB_DEFAULT_NCORES

    version = args.version
    if version == "mpi" or version == "run_bb_mpi":
        sl_name_prefix = "run_bb_mpi"
    else:
        if version is not None:
            print("% cannot be recognize as a valide option" % version)
            print("version is set to default: %", const.BB_DEFAULT_VERSION)
        version = const.BB_DEFAULT_VERSION
        sl_name_prefix = const.BB_DEFAULT_VERSION
    print(version)

    srf_name = os.path.splitext(os.path.basename(params.srf_file))[0]
    if args.srf is None or srf_name == args.srf:

        # TODO: save status as HF. refer to submit_hf

        # Use HF nt for wct estimation
        nt = get_nt(params)
        fd_count = len(shared.get_stations(params.FD_STATLIST))
        est_core_hours, est_run_time = est.est_BB_chours_single(fd_count, nt, ncores)
        wct = set_wct(est_run_time, ncores, args.auto)
        bb_sim_dir = os.path.join(params.sim_dir, "BB")
        write_directory = args.write_directory if args.write_directory else params.sim_dir
        underscored_srf = srf_name.replace("/", "__")

        header_dict = {
            "account": args.account,
            "n_tasks": ncores,
            "wallclock_limit": wct,
            "job_name": "sim_bb.{}".format(underscored_srf),
            "version": "slurm",
            "memory": const.DEFAULT_MEMORY,
            "exe_time": const.timestamp,
            "job_description": "BB calculation",
            "additional_lines": "###SBATCH -C avx",
            "target_host": args.machine,
            "write_directory": write_directory,
        }

        template_parameters = {
            "outbin_dir": os.path.join(params.sim_dir, "LF", "OutBin"),
            "vel_mod_dir": params.vel_mod_dir,
            "hf_bin_path": os.path.join(params.sim_dir, "HF", "Acc/HF.bin"),
            "stat_vs_est": params.stat_vs_est,
            "bb_bin_path": os.path.join(bb_sim_dir, "Acc/BB.bin"),
            "flo": params.flo,
        }

        body_dict = {
            "simulation_dir": params.sim_dir,
            "template_path": "{}.sl.template".format(sl_name_prefix),
            "rup_mod": underscored_srf,
            "mgmt_db_location": params.mgmt_db_location,
            "sim_dir": params.sim_dir,
            "srf_name": srf_name,
            "test_bb_script": "test_bb_binary.sh",
        }

        script_prefix = "{}_{}".format(sl_name_prefix, underscored_srf)
        script_file_path = write_sl_script(write_directory, params.sim_dir, const.ProcessType.BB, script_prefix,
                                           header_dict, body_dict, BB_COMMAND_TEMPLATE, template_parameters, params.bb)

        # Submit the script
        submit_yes = True if args.auto else confirm("Also submit the job for you?")
        submit_sl_script(
            script_file_path,
            "BB",
            "queued",
            params.mgmt_db_location,
            srf_name,
            const.timestamp,
            submit_yes=submit_yes,
            target_machine=args.machine,
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Create (and submit if specified) the slurm script for BB"
    )

    parser.add_argument("--auto", nargs="?", type=str, const=True)
    parser.add_argument("--version", type=str, default=const.BB_DEFAULT_VERSION)
    parser.add_argument("--account", type=str, default=const.DEFAULT_ACCOUNT)
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
    args = parser.parse_args()

    main(args)
