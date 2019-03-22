#!/usr/bin/env python3
"""Script to create/submit simulation IM calculation"""
import os
import argparse
from enum import Enum

import qcore.constants as const
from qcore import utils, shared
from qcore.config import host
from typing import Dict
from estimation.estimate_wct import est_IM_chours_single
from shared_workflow.shared import (
    submit_sl_script,
    set_wct,
    confirm,
    resolve_header,
    generate_context,
)


class SlHdrOptConsts(Enum):
    job_name_prefix = "job_name_prefix"
    account = "account"
    n_tasks = "n_tasks"
    memory = "memory"
    additional = "additional"
    wallclock = "wallclock_limit"
    version = "version"
    description = "description"


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
    SlBodyOptConsts.n_procs.value: const.IM_CALC_DEFAULT_N_PROCESSES,
    SlBodyOptConsts.extended.value: False,
    SlBodyOptConsts.simple_out.value: True,
    "auto": False,
    "machine": host,
    "write_directory": None,
}


def submit_im_calc_slurm(sim_dir: str, options_dict: Dict = None):
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

    # Get wall clock estimation
    print("Running wall clock estimation for IM sim")
    est_core_hours, est_run_time = est_IM_chours_single(
        len(shared.get_stations(params.FD_STATLIST)),
        int(float(params.sim_duration) / float(params.hf.dt)),
        [options_dict[SlBodyOptConsts.component.value]],
        100 if options_dict[SlBodyOptConsts.extended.value] else 15,
        options_dict[SlBodyOptConsts.n_procs.value],
    )
    wct = set_wct(
        est_run_time, options_dict[SlBodyOptConsts.n_procs.value], options_dict["auto"]
    )

    with open("sim_im_calc.sl.template", "r") as f:
        template = f.read()

    extended = "-e" if options_dict[SlBodyOptConsts.extended.value] else ""
    simple = "-s" if options_dict[SlBodyOptConsts.simple_out.value] else ""
    template = generate_context(
        sim_dir,
        "sim_im_calc.sl.template",
        component=options_dict[SlBodyOptConsts.component.value],
        sim_dir=sim_dir,
        sim_name=sim_name,
        fault_name=fault_name,
        np=options_dict[SlBodyOptConsts.n_procs.value],
        extended=extended,
        simple=simple,
        mgmt_db_location=params.mgmt_db_location,
    )

    # slurm header
    header = resolve_header(
        options_dict[SlHdrOptConsts.account.value],
        options_dict[SlHdrOptConsts.n_tasks.value],
        wct,
        "{}_{}".format(options_dict[SlHdrOptConsts.job_name_prefix.value], fault_name),
        options_dict[SlHdrOptConsts.description.value],
        options_dict[SlHdrOptConsts.memory.value],
        const.timestamp,
        job_description=options_dict[SlHdrOptConsts.description.value],
        additional_lines=options_dict[SlHdrOptConsts.additional.value],
        target_host=options_dict["machine"],
        write_directory=options_dict["write_directory"],
    )

    script = os.path.abspath(
        os.path.join(
            options_dict["write_directory"],
            const.IM_SIM_SL_SCRIPT_NAME.format(const.timestamp),
        )
    )

    # Write the script
    with open(script, "w") as f:
        f.write(header)
        f.write("\n")
        f.write(template)

    submit_yes = (
        True if options_dict["auto"] else confirm("Also submit the job for you?")
    )
    submit_sl_script(
        script,
        "IM_calculation",
        "queued",
        params.mgmt_db_location,
        os.path.splitext(os.path.basename(params.srf_file))[0],
        const.timestamp,
        submit_yes=submit_yes,
        target_machine=options_dict["machine"],
    )

    return script


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
