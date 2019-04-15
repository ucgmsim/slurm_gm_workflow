#!/usr/bin/env python3
"""Script to create/submit simulation IM calculation"""
import os
import argparse
from enum import Enum

import qcore.constants as const
import qcore.simulation_structure as sim_struct
from qcore import utils, shared
from qcore.config import host
from typing import Dict
from estimation.estimate_wct import est_IM_chours_single, EstModel
from qcore.utils import DotDictify
from shared_workflow.load_config import load
from shared_workflow.shared import submit_sl_script, set_wct, confirm, write_sl_script


class SlHdrOptConsts(Enum):
    job_name_prefix = "job_name_prefix"
    account = "account"
    n_tasks = "n_tasks"
    memory = "memory"
    additional = "additional_lines"
    wallclock = "wallclock_limit"
    version = "version"
    description = "job_description"


class SlBodyOptConsts(Enum):
    component = "component"
    n_procs = "np"
    sim_dir = "sim_dir"
    sim_name = "sim_name"
    fault_name = "fault_name"
    output_dir = "output_dir"
    extended = "extended"
    simple_out = "simple"
    mgmt_db = "mgmt_db"


DEFAULT_OPTIONS = {
    # Header
    SlHdrOptConsts.job_name_prefix.value: "sim_im_calc",
    SlHdrOptConsts.description.value: "Calculates intensity measures.",
    SlHdrOptConsts.account.value: "nesi00213",
    SlHdrOptConsts.additional.value: "#SBATCH --hint=nomultithread",
    SlHdrOptConsts.memory.value: "2G",
    SlHdrOptConsts.n_tasks.value: 1,
    SlHdrOptConsts.version.value: "slurm",
    # Body
    SlBodyOptConsts.component.value: const.IM_CALC_COMPONENTS[0],
    SlBodyOptConsts.n_procs.value: const.IM_CALC_DEFAULT_N_CORES,
    SlBodyOptConsts.extended.value: False,
    SlBodyOptConsts.simple_out.value: True,
    "auto": False,
    "machine": host,
    "write_directory": None,
}


def submit_im_calc_slurm(sim_dir: str, options_dict: Dict = None, est_model: EstModel = None):
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
    sim_name = os.path.basename(sim_dir)
    fault_name = sim_name.split("_")[0]

    if est_model is None:
        workflow_config = load(
            os.path.dirname(os.path.realpath(__file__)), "workflow_config.json"
        )
        est_model = os.path.join(workflow_config["estimation_models_dir"], "IM")

    # Get wall clock estimation
    print("Running wall clock estimation for IM sim")
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

    header_dict = {
        "wallclock_limit": wct,
        "job_name": "{}_{}".format(
            options_dict[SlHdrOptConsts.job_name_prefix.value], fault_name
        ),
        "exe_time": const.timestamp,
        "target_host": options_dict["machine"],
        "write_directory": options_dict["write_directory"],
        "n_tasks": options_dict[SlHdrOptConsts.n_tasks.value],
        "job_description": options_dict[SlHdrOptConsts.description.value],
    }

    command_template_parameters = {
        "sim_dir": sim_dir,
        "component": options_dict[SlBodyOptConsts.component.value],
        "sim_name": sim_name,
        "fault_name": fault_name,
        "np": options_dict[SlBodyOptConsts.n_procs.value],
        "extended": "-e" if options_dict[SlBodyOptConsts.extended.value] else "",
        "simple": "-s" if options_dict[SlBodyOptConsts.simple_out.value] else "",
    }

    body_template_params = (
        "sim_im_calc.sl.template",
        {
            "component": options_dict[SlBodyOptConsts.component.value],
            "sim_name": sim_name,
            "fault_name": fault_name,
            "np": options_dict[SlBodyOptConsts.n_procs.value],
        },
    )

    script_prefix = "sim_im_calc"
    script_file_path = write_sl_script(
        options_dict["write_directory"],
        sim_dir,
        const.ProcessType.IM_calculation,
        script_prefix,
        header_dict,
        body_template_params,
        command_template_parameters,
        DotDictify(
            {
                "account": options_dict[SlHdrOptConsts.account.value],
                "machine": options_dict["machine"],
            }
        ),
    )

    submit_yes = (
        True if options_dict["auto"] else confirm("Also submit the job for you?")
    )

    submit_sl_script(
        script_file_path,
        const.ProcessType.IM_calculation.value,
        sim_struct.get_mgmt_db_queue(params.mgmt_db_location),
        os.path.splitext(os.path.basename(params.srf_file))[0],
        submit_yes=submit_yes,
        target_machine=options_dict["machine"],
    )

    return script_file_path


def main(args):
    if not args.comp in const.IM_CALC_COMPONENTS:
        parser.error(
            "Velocity component must be in {}, where ellipsis means calculating "
            "all components".format(const.IM_CALC_COMPONENTS)
        )

    submit_im_calc_slurm(
        args.sim_dir,
        {
            SlBodyOptConsts.n_procs.value: args.n_procs,
            SlBodyOptConsts.extended.value: args.extended_period,
            SlBodyOptConsts.simple_out.value: args.simple_output,
            SlBodyOptConsts.component.value: args.comp,
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
        default=const.IM_CALC_DEFAULT_N_PROCESSES,
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
        default=const.IM_CALC_COMPONENTS[0],
        help="specify which velocity component to calculate. choose from {}. Default is {}".format(
            const.IM_CALC_COMPONENTS, const.IM_CALC_COMPONENTS[0]
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

    args = parser.parse_args()

    main(args)
