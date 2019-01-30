#!/usr/bin/env python3
"""Script to create/submit simulation IM calculation"""
import os
import argparse
from enum import Enum
from datetime import datetime

from jinja2 import Environment, FileSystemLoader

from qcore import utils, shared
from typing import Dict
from estimation.estimate_wct import est_IM_chours_single
from shared_workflow.shared import submit_sl_script, set_wct, confirm

IM_CALC_DEFAULT_N_PROCESSES = 40
IM_CALC_COMPONENTS = ["geom", "000", "090", "ver", "ellipsis"]

IM_CALC_TEMPLATE_NAME = "sim_im_calc.sl.template"
HEADER_TEMPLATE = "slurm_header.cfg"

SL_SCRIPT_NAME = "sim_im_calc_{}.sl"


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
    SlBodyOptConsts.component.value: IM_CALC_COMPONENTS[0],
    SlBodyOptConsts.n_procs.value: IM_CALC_DEFAULT_N_PROCESSES,
    SlBodyOptConsts.extended.value: False,
    SlBodyOptConsts.simple_out.value: False,
    "auto": False,
}


def submit_im_calc_slurm(sim_dir: str, options_dict: Dict = None):
    # Load the yaml params if they haven't been loaded already
    params = utils.load_sim_params(
        os.path.join(sim_dir, "sim_params.yaml"), load_vm=False
    )

    options_dict = {**DEFAULT_OPTIONS, **options_dict}
    sim_name = os.path.basename(sim_dir)
    fault_name = sim_name.split("_")[0]

    # Get wall clock estimation
    print("Running wall clock estimation for IM sim")
    est_core_hours, est_run_time = est_IM_chours_single(
        len(shared.get_stations(params.FD_STATLIST)),
        int(float(params.sim_duration) / float(params.hf.hf_dt)),
        [options_dict[SlBodyOptConsts.component.value]],
        100 if options_dict[SlBodyOptConsts.extended.value] else 15,
        options_dict[SlBodyOptConsts.n_procs.value],
    )
    wct = set_wct(
        est_run_time, options_dict[SlBodyOptConsts.n_procs.value], options_dict["auto"]
    )

    # Header
    j2_env = Environment(
        loader=FileSystemLoader(sim_dir),
        trim_blocks=True,
    )
    header = j2_env.get_template(HEADER_TEMPLATE).render(
        version=options_dict[SlHdrOptConsts.description.value],
        job_description=options_dict[SlHdrOptConsts.description.value],
        job_name="{}_{}".format(
            options_dict[SlHdrOptConsts.job_name_prefix.value], fault_name
        ),
        account=options_dict[SlHdrOptConsts.account.value],
        n_tasks=options_dict[SlHdrOptConsts.n_tasks.value],
        wallclock_limit=wct,
        exe_time="%j",
        mail="test@test.com",
        memory=options_dict[SlHdrOptConsts.memory.value],
        additional_lines=options_dict[SlHdrOptConsts.additional.value],
    )

    body = j2_env.get_template(IM_CALC_TEMPLATE_NAME).render(
        component=options_dict[SlBodyOptConsts.component.value],
        sim_dir=sim_dir,
        sim_name=sim_name,
        fault_name=fault_name,
        np=options_dict[SlBodyOptConsts.n_procs.value],
        extended="-e" if options_dict[SlBodyOptConsts.extended.value] else "",
        simple="-s" if options_dict[SlBodyOptConsts.simple_out.value] else "",
        mgmt_db_location=params.mgmt_db_location,
    )

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    script = os.path.join(sim_dir, SL_SCRIPT_NAME.format(timestamp))

    # Write the script
    with open(script, "w") as f:
        f.write(header)
        f.write("\n")
        f.write(body)

    submit_yes = (
        True if options_dict["auto"] else confirm("Also submit the job for you?")
    )
    submit_sl_script(
        script,
        "IM_calc",
        "queued",
        params.mgmt_db_location,
        os.path.splitext(os.path.basename(params.srf_file))[0],
        timestamp,
        submit_yes=submit_yes,
    )

    return script


def main(args):
    if not args.comp in IM_CALC_COMPONENTS:
        parser.error(
            "Velocity component must be in {}, where ellipsis means calculating "
            "all components".format(IM_CALC_COMPONENTS)
        )

    script = submit_im_calc_slurm(
        args.sim_dir,
        {
            SlBodyOptConsts.n_procs.value: args.n_procs,
            SlBodyOptConsts.extended.value: args.extended_period,
            SlBodyOptConsts.simple_out.value: args.simple_output,
            SlBodyOptConsts.component.value: args.comp,
            "auto": args.auto,
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
        default=IM_CALC_DEFAULT_N_PROCESSES,
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
        default=IM_CALC_COMPONENTS[0],
        help="specify which velocity component to calculate. choose from {}. Default is {}".format(
            IM_CALC_COMPONENTS, IM_CALC_COMPONENTS[0]
        ),
    )
    parser.add_argument(
        "--auto",
        action="store_true",
        default=False,
        help="Submit the slurm script automatically and use the "
        "estimated wct. No prompts.",
    )

    args = parser.parse_args()

    main(args)