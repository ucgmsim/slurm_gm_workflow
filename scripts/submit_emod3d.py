#!/usr/bin/env python3
"""Script to create and submit a slurm script for LF"""
# TODO: import the CONFIG here
# Section for parser to determine if using automate wct
import os
import argparse
from logging import Logger

import scripts.set_runparams as set_runparams
import qcore.constants as const
import qcore.simulation_structure as sim_struct
import estimation.estimate_wct as est
from qcore import utils, binary_version
from qcore.config import get_machine_config, host
from shared_workflow import load_config
from shared_workflow.shared import (
    confirm,
    set_wct,
    submit_sl_script,
    get_nt,
)
from shared_workflow.shared_template import write_sl_script

# Estimated number of minutes between each checkpoint
from shared_workflow.workflow_logger import get_basic_logger

CHECKPOINT_DURATION = 10


def main(args: argparse.Namespace, est_model: est.EstModel = None, logger: Logger = get_basic_logger()):
    params = utils.load_sim_params(os.path.join(args.rel_dir, "sim_params.yaml"))

    submit_yes = True if args.auto else confirm("Also submit the job for you?")

    workflow_config = load_config.load(
        os.path.dirname(os.path.realpath(__file__)), "workflow_config.json"
    )

    logger.debug("params.srf_file {}".format(params.srf_file))
    # Get the srf(rup) name without extensions
    srf_name = os.path.splitext(os.path.basename(params.srf_file))[0]
    if args.srf is None or srf_name == args.srf:
        logger.debug("not set_params_only")
        # get lf_sim_dir
        sim_dir = os.path.abspath(params.sim_dir)
        lf_sim_dir = os.path.join(sim_dir, "LF")

        # default_core will be changed is user passes ncore
        model = (
            est_model
            if est_model is not None
            else os.path.join(workflow_config["estimation_models_dir"], "LF")
        )
        est_core_hours, est_run_time, est_cores = est.est_LF_chours_single(
            int(params.nx),
            int(params.ny),
            int(params.nz),
            get_nt(params),
            args.ncore,
            model,
            True,
        )
        wct = set_wct(est_run_time, est_cores, args.auto)

        target_qconfig = get_machine_config(args.machine)

        binary_path = binary_version.get_lf_bin(
            params.emod3d.emod3d_version, target_qconfig["tools_dir"]
        )
        steps_per_checkpoint = int(
            get_nt(params) / (60.0 * est_run_time) * CHECKPOINT_DURATION
        )
        write_directory = (
            args.write_directory if args.write_directory else params.sim_dir
        )

        set_runparams.create_run_params(
            sim_dir,
            workflow_config=workflow_config,
            steps_per_checkpoint=steps_per_checkpoint,
            logger=logger
        )

        header_dict = {
            "n_tasks": est_cores,
            "wallclock_limit": wct,
            "job_name": "run_emod3d.{}".format(srf_name),
            "job_description": "emod3d slurm script",
            "additional_lines": "#SBATCH --hint=nomultithread",
        }

        command_template_parameters = {
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
            args,
        )

        submit_sl_script(
            script_file_path,
            const.ProcessType.EMOD3D.value,
            sim_struct.get_mgmt_db_queue(params.mgmt_db_location),
            srf_name,
            submit_yes=submit_yes,
            target_machine=args.machine,
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Create (and submit if specified) the slurm script for LF"
    )

    parser.add_argument("--ncore", type=int, default=const.LF_DEFAULT_NCORES)
    parser.add_argument("--auto", nargs="?", type=str, const=True)
    parser.add_argument("--account", type=str, default=const.DEFAULT_ACCOUNT)
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
