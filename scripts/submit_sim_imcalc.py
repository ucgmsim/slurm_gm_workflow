#!/usr/bin/env python3
"""Script to create/submit simulation IM calculation"""
import os
import argparse
from enum import Enum
from logging import Logger
from typing import Dict

from qcore import utils, shared
from qcore.config import host
import qcore.constants as const
from qcore.qclogging import get_basic_logger
import qcore.simulation_structure as sim_struct

from estimation.estimate_wct import est_IM_chours_single, EstModel
from scripts.schedulers.scheduler_factory import Scheduler
from shared_workflow.platform_config import (
    platform_config,
    get_platform_node_requirements,
)
from shared_workflow.shared import set_wct, confirm
from shared_workflow.shared_automated_workflow import submit_script_to_scheduler
from shared_workflow.shared_template import write_sl_script

# from IM_calculation.Advanced_IM import advanced_IM_factory


class SlHdrOptConsts(Enum):
    job_name_prefix = "job_name_prefix"
    account = "account"
    n_tasks = "n_tasks"
    memory = "memory"
    additional = "additional_lines"
    wallclock = "wallclock_limit"
    version = "version"
    description = "job_description"


# these values has to match the command_template in qcore.const.ProcessType
# TODO: move these classes to qcore.constant
class SlBodyOptConsts(Enum):
    component = "component"
    n_procs = "np"
    sim_dir = "sim_dir"
    sim_name = "sim_name"
    fault_name = "fault_name"
    output_dir = "output_dir"
    extended = "extended"
    simple_out = "simple"
    advanced_IM = const.ProcessType.advanced_IM.str_value
    mgmt_db = "mgmt_db"


DEFAULT_OPTIONS = {
    # Header
    SlHdrOptConsts.job_name_prefix.value: "sim_im_calc",
    SlHdrOptConsts.description.value: "Calculates intensity measures.",
    SlHdrOptConsts.account.value: platform_config[
        const.PLATFORM_CONFIG.DEFAULT_ACCOUNT.name
    ],
    SlHdrOptConsts.additional.value: "#SBATCH --hint=nomultithread",
    SlHdrOptConsts.memory.value: "2G",
    SlHdrOptConsts.n_tasks.value: 1,
    SlHdrOptConsts.version.value: "slurm",
    # Body
    SlBodyOptConsts.component.value: const.Components.cgeom.str_value,
    SlBodyOptConsts.n_procs.value: platform_config[
        const.PLATFORM_CONFIG.IM_CALC_DEFAULT_N_CORES.name
    ],
    SlBodyOptConsts.extended.value: False,
    SlBodyOptConsts.simple_out.value: True,
    "auto": False,
    "machine": host,
    "write_directory": None,
    "OpenSees_ver": "OpenSees/3.0.0-gimkl-2017a",
    SlBodyOptConsts.advanced_IM.value: None,
}


def submit_im_calc_slurm(
    sim_dir: str,
    options_dict: Dict = None,
    est_model: EstModel = None,
    logger: Logger = get_basic_logger(),
):
    """Creates the IM calc slurm scrip, also submits if specified

    The options_dict is populated by the DEFAULT_OPTIONS, values can be changed by
    passing in a dict containing the entries that require changing. Merges the
    two dictionaries, the passed in one has higher priority.
    """
    # Load the yaml params if they haven't been loaded already
    params = utils.load_sim_params(
        os.path.join(sim_dir, "sim_params.yaml"), load_vm=False
    )

    options_dict = {**DEFAULT_OPTIONS, **options_dict}
    if options_dict["write_directory"] is None:
        options_dict["write_directory"] = sim_dir
    if options_dict[SlBodyOptConsts.advanced_IM.value]:
        # TODO: update this to use number from adv_im estmation model after it exist
        options_dict[SlBodyOptConsts.n_procs.value] = 18
        options_dict[SlHdrOptConsts.n_tasks.value] = options_dict[
            SlBodyOptConsts.n_procs.value
        ]

    sim_name = os.path.basename(sim_dir)
    fault_name = sim_name.split("_")[0]
    proc_type = (
        const.ProcessType.advanced_IM
        if options_dict[SlBodyOptConsts.advanced_IM.value]
        else const.ProcessType.IM_calculation
    )

    if est_model is None:
        est_model = os.path.join(
            platform_config[const.PLATFORM_CONFIG.ESTIMATION_MODELS_DIR.name], "IM"
        )

    # Get wall clock estimation
    logger.info(
        "Running wall clock estimation for IM sim for realisation {}".format(sim_name)
    )
    est_core_hours, est_run_time = est_IM_chours_single(
        len(shared.get_stations(params.FD_STATLIST)),
        int(float(params.sim_duration) / float(params.hf.dt)),
        [options_dict[SlBodyOptConsts.component.value]],
        100 if options_dict[SlBodyOptConsts.extended.value] else 15,
        options_dict[SlBodyOptConsts.n_procs.value],
        est_model,
    )

    wct = set_wct(
        est_run_time, options_dict[SlBodyOptConsts.n_procs.value], options_dict["auto"]
    )

    # adv_im related check

    header_dict = {
        "wallclock_limit": wct,
        "job_name": "{}_{}".format(
            # options_dict[SlHdrOptConsts.job_name_prefix.value], fault_name
            proc_type.str_value,
            fault_name,
        ),
        "exe_time": const.timestamp,
        "write_directory": options_dict["write_directory"],
        "platform_specific_args": get_platform_node_requirements(
            options_dict[SlHdrOptConsts.n_tasks.value]
        ),
        "job_description": options_dict[SlHdrOptConsts.description.value],
        # TODO: this logic may need update along with adv_im_est_model
        SlHdrOptConsts.additional.value: options_dict[SlHdrOptConsts.additional.value]
        + "\n#SBATCH --nodes=1"
        if options_dict[SlBodyOptConsts.advanced_IM.value]
        else options_dict[SlHdrOptConsts.additional.value],
    }

    # construct parameters thats matches qcore.const.ProccessType.command_template
    command_template_parameters = {
        SlBodyOptConsts.sim_dir.value: sim_dir,
        SlBodyOptConsts.component.value: "-c {}".format(
            (options_dict[SlBodyOptConsts.component.value])
        )
        if not options_dict[SlBodyOptConsts.advanced_IM.value]
        else "",
        SlBodyOptConsts.sim_name.value: sim_name,
        SlBodyOptConsts.fault_name.value: fault_name,
        SlBodyOptConsts.n_procs.value: options_dict[SlBodyOptConsts.n_procs.value],
        SlBodyOptConsts.extended.value: "-e"
        if options_dict[SlBodyOptConsts.extended.value]
        else "",
        SlBodyOptConsts.simple_out.value: "-s"
        if options_dict[SlBodyOptConsts.simple_out.value]
        else "",
        SlBodyOptConsts.advanced_IM.value: "-a {}".format(
            " ".join(options_dict[SlBodyOptConsts.advanced_IM.value])
        )
        if options_dict[SlBodyOptConsts.advanced_IM.value]
        else "",
        "pSA_periods": f"-p {' '.join(str(p) for p in params['pSA_periods'])}"
        if "pSA_periods" in params
        else "",
    }

    # determine script template based on advanced_IM or not
    if options_dict[SlBodyOptConsts.advanced_IM.value]:
        sl_template = "adv_im_calc.sl.template"
        script_prefix = "adv_im_calc"
    else:
        sl_template = "sim_im_calc.sl.template"
        script_prefix = "sim_im_calc"

    body_template_params = (
        sl_template,
        {
            "component": options_dict[SlBodyOptConsts.component.value],
            "sim_name": sim_name,
            "fault_name": fault_name,
            "np": options_dict[SlBodyOptConsts.n_procs.value],
            "output_csv": sim_struct.get_IM_csv(sim_dir),
            "output_info": sim_struct.get_IM_info(sim_dir),
            "models": " ".join(options_dict[SlBodyOptConsts.advanced_IM.value])
            if options_dict[SlBodyOptConsts.advanced_IM.value]
            else None,
        },
    )
    script_file_path = write_sl_script(
        options_dict["write_directory"],
        sim_dir,
        proc_type,
        script_prefix,
        header_dict,
        body_template_params,
        command_template_parameters,
    )

    submit_yes = (
        True if options_dict["auto"] else confirm("Also submit the job for you?")
    )

    if submit_yes:
        submit_script_to_scheduler(
            script_file_path,
            proc_type.value,
            sim_struct.get_mgmt_db_queue(params.mgmt_db_location),
            sim_dir,
            os.path.splitext(os.path.basename(params.srf_file))[0],
            target_machine=options_dict["machine"],
            logger=logger,
        )

    return script_file_path


def main(args):
    submit_im_calc_slurm(
        args.sim_dir,
        {
            SlBodyOptConsts.n_procs.value: args.n_procs,
            SlBodyOptConsts.extended.value: args.extended_period,
            SlBodyOptConsts.simple_out.value: args.simple_output,
            SlBodyOptConsts.component.value: args.comp,
            # SlBodyOptConsts.advanced_IM.value: args.advanced_ims,
            "auto": args.auto,
            "machine": args.machine,
            "write_directory": args.write_directory,
        },
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--sim_dir",
        type=str,
        help="The simulation directory to calculate the IMs for. Defaults to the current directory.",
        default=os.getcwd(),
    )
    parser.add_argument(
        "--n_procs",
        type=int,
        help="Number of processes to use",
        default=platform_config[const.PLATFORM_CONFIG.IM_CALC_DEFAULT_N_CORES.name],
    )
    parser.add_argument(
        "-e",
        "--extended_period",
        action="store_true",
        default=False,
        help="If specified extended pSA periods are calculated",
    )
    parser.add_argument(
        "-s",
        "--simple_output",
        action="store_true",
        default=False,
        help="If specfied a single csv is generated, instead of one for each station",
    )
    parser.add_argument(
        "-c",
        "--comp",
        nargs="*",
        choices=list(const.Components.iterate_str_values()),
        default=[const.Components.cgeom.str_value],
        help="specify which velocity component to calculate. choose from {}. Default is {}".format(
            ", ".join((list(const.Components.iterate_str_values()))),
            const.Components.cgeom.str_value,
        ),
    )
    parser.add_argument(
        "--auto",
        action="store_true",
        default=False,
        help="Submit the slurm script automatically and use the "
        "estimated wct. No prompts.",
    )
    parser.add_argument(
        "--machine",
        type=str,
        default=host,
        help="The machine sim_imcalc is to be submitted to.",
    )
    parser.add_argument(
        "--write_directory",
        type=str,
        help="The directory to write the slurm script to.",
        default=None,
    )

    # parser.add_argument(
    #     "-a",
    #     "--advanced_ims",
    #     nargs="+",
    #     choices=advanced_IM_factory.get_im_list(parent_args[0].advanced_im_config),
    #     help="Provides the list of Advanced IMs to be calculated",
    # )

    args = parser.parse_args()

    # The name parameter is only used to check user tasks in the queue monitor
    Scheduler.initialise_scheduler("", args.account)

    main(args)
