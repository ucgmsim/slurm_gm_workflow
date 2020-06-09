#!/usr/bin/env python3
"""Script to create and submit a slurm script for LF"""
# TODO: import the CONFIG here
# Section for parser to determine if using automate wct
import os
import argparse
from logging import Logger

from qcore import utils, binary_version
from qcore.config import get_machine_config, host
from qcore.qclogging import get_basic_logger
import qcore.constants as const
import qcore.simulation_structure as sim_struct

import estimation.estimate_wct as est
import scripts.set_runparams as set_runparams
from shared_workflow.platform_config import (
    platform_config,
    get_platform_node_requirements,
)
from shared_workflow.shared import confirm, set_wct
from shared_workflow.shared_automated_workflow import submit_sl_script
from shared_workflow.shared_template import write_sl_script


def main(
    args: argparse.Namespace,
    est_model: est.EstModel = None,
    logger: Logger = get_basic_logger(),
):
    params = utils.load_sim_params(os.path.join(args.rel_dir, "sim_params.yaml"))

    submit_yes = True if args.auto else confirm("Also submit the job for you?")

    logger.debug("params.srf_file {}".format(params.srf_file))
    # Get the srf(rup) name without extensions
    srf_name = os.path.splitext(os.path.basename(params.srf_file))[0]
    if args.srf is None or srf_name == args.srf:
        logger.debug("not set_params_only")
        # get lf_sim_dir
        sim_dir = os.path.abspath(params.sim_dir)
        lf_sim_dir = sim_struct.get_lf_dir(sim_dir)

        # default_core will be changed is user passes ncore
        nt = int(float(params.sim_duration) / float(params.dt))
        model = (
            est_model
            if est_model is not None
            else os.path.join(
                platform_config[const.PLATFORM_CONFIG.ESTIMATION_MODELS_DIR.name], "LF"
            )
        )
        est_core_hours, est_run_time, est_cores = est.est_LF_chours_single(
            int(params.nx), int(params.ny), int(params.nz), nt, args.ncore, model, True
        )
        # scale up the est_run_time if it is a re-run (with check-pointing)
        # otherwise do nothing
        est_run_time_scaled = est_run_time
        if hasattr(args, "retries"):
            # quick sanity check
            # TODO: combine this function with 'restart' check in run_emod3d.sl.template
            lf_restart_dir = sim_struct.get_lf_restart_dir(sim_dir)
            # check if the restart folder exist and has checkpointing files in it
            if os.path.isdir(lf_restart_dir) and (len(os.listdir(lf_restart_dir)) > 0):
                # scale up the wct with retried count
                est_run_time_scaled = est_run_time * (int(args.retries) + 1)
            else:
                logger.debug(
                    "retries has been set, but no check-pointing files exist. not scaling wct"
                )

        wct = set_wct(est_run_time_scaled, est_cores, args.auto)

        target_qconfig = get_machine_config(args.machine)

        binary_path = binary_version.get_lf_bin(
            params.emod3d.emod3d_version, target_qconfig["tools_dir"]
        )
        # use the original estimated run time for determining the checkpoint
        steps_per_checkpoint = int(
            nt / (60.0 * est_run_time) * const.CHECKPOINT_DURATION
        )
        write_directory = (
            args.write_directory if args.write_directory else params.sim_dir
        )

        set_runparams.create_run_params(
            sim_dir, steps_per_checkpoint=steps_per_checkpoint, logger=logger
        )

        header_dict = {
            "wallclock_limit": wct,
            "job_name": "run_emod3d.{}".format(srf_name),
            "job_description": "emod3d slurm script",
            "additional_lines": "#SBATCH --hint=nomultithread",
            "platform_specific_args": get_platform_node_requirements(est_cores),
        }

        command_template_parameters = {
            "run_command": platform_config[const.PLATFORM_CONFIG.RUN_COMMAND.name],
            "emod3d_bin": binary_path,
            "lf_sim_dir": lf_sim_dir,
        }

        body_template_params = ("run_emod3d.sl.template", {})

        script_prefix = "run_emod3d_{}".format(srf_name)
        script_file_path = write_sl_script(
            write_directory,
            params.sim_dir,
            const.ProcessType.EMOD3D,
            script_prefix,
            header_dict,
            body_template_params,
            command_template_parameters,
        )
        if submit_yes:
            submit_sl_script(
                script_file_path,
                const.ProcessType.EMOD3D.value,
                sim_struct.get_mgmt_db_queue(params.mgmt_db_location),
                params.sim_dir,
                srf_name,
                target_machine=args.machine,
                logger=logger,
            )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Create (and submit if specified) the slurm script for LF"
    )

    parser.add_argument(
        "--ncore",
        type=int,
        default=platform_config[const.PLATFORM_CONFIG.LF_DEFAULT_NCORES.name],
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
        help="The machine emod3d is to be submitted to.",
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
