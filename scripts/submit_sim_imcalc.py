#!/usr/bin/env python3
"""Script to create/submit simulation IM calculation"""
import os
import argparse
from enum import Enum

from jinja2 import Template, Environment, FileSystemLoader

from qcore import utils, shared
from typing import Dict
from estimation.estimate_wct import est_IM_chours_single
from shared_workflow.shared import exe, submit_sl_script, update_db_cmd, set_wct

IM_CALC_DEFAULT_N_PROCESSES = 40
IM_CALC_COMPONENTS = ["geom", "000", "090", "ver", "ellipsis"]

IM_CALC_TEMPLATE_NAME = "im_calc.sl.template"
HEADER_TEMPLATE = "slurm_header.cfg"


# These things belong in a constants file
class SlHdrOptConsts(Enum):
    job_name_prefix = "job_name_prefix"
    account = "account"
    n_tasks = "n_tasks"
    memory = "memory"
    additional = "additional"
    wallclock = "wallclock_limit"
    version = "version"
    description = "description"

DEFAULT_SLURM_OPTIONS = {
    SlHdrOptConsts.job_name_prefix.value: "im_calc",
    SlHdrOptConsts.description.value: "Calculates intensity measures.",
    SlHdrOptConsts.account.value: "nesi00213",
    SlHdrOptConsts.additional.value: "#SBATCH --hint=nomultithread",
    SlHdrOptConsts.memory.value: "2G",
    SlHdrOptConsts.n_tasks.value: 1,
    SlHdrOptConsts.version.value: "slurm"
}


def create_im_calc_slurm(sim_dir: str, slurm_options: Dict = None):
    # Load the yaml params
    params = utils.load_sim_params(
        os.path.join(sim_dir, "sim_params.yaml"),
        load_vm=False,
    )

    # Get wall clock estimation
    print("Running wall clock estimation for IM sim")
    est_core_hours, est_run_time = est_IM_chours_single(
        len(shared.get_stations(params.FD_STATLIST)),
        int(float(params.sim_duration) / float(params.hf.hf_dt)),
        [args.comp],
        100 if args.extended_period else 15,
        args.processes,
    )
    wct = set_wct(est_run_time, args.processes, args.auto)

    slurm_options = {**DEFAULT_SLURM_OPTIONS, **slurm_options}
    fault_name = os.path.basename(sim_dir).split("_")[0]

    # Header
    j2_env = Environment(loader=FileSystemLoader(os.path.join(sim_dir, IM_CALC_TEMPLATE_NAME)), trim_blocks=True)
    header = j2_env.get_template(HEADER_TEMPLATE).render(
        version=slurm_options[SlHdrOptConsts.description.value],
        job_description=slurm_options[SlHdrOptConsts.description.value],
        job_name="{}_{}".format(slurm_options[SlHdrOptConsts.job_name_prefix.value], fault_name),
        account=slurm_options[SlHdrOptConsts.account.value],
        n_tasks= slurm_options[SlHdrOptConsts.n_tasks.value],
        wallclock_limit=wct,
        exe_time="%j",
        mail="test@test.com",
        memory=slurm_options[SlHdrOptConsts.memory.value],
        additional_lines=slurm_options[SlHdrOptConsts.additionals.value],
    )

    context = j2_env.get_template(os.path.join(sim_dir, IM_CALC_TEMPLATE_NAME)).render(
        comp=comp,
        sim_dirs=sim_dirs,
        obs_dirs=obs_dirs,
        rrup_files=rrup_files,
        station_file=station_file,
        output_dir=output_dir,
        np=n_procs,
        extended=extended,
        simple=simple,
        mgmt_db_location=mgmt_db,
    )


def main(args):
    if not args.comp in IM_CALC_COMPONENTS:
        parser.error(
            "Velocity component must be in {}, where ellipsis means calculating "
            "all compoents".format(IM_CALC_COMPONENTS)
        )



if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "sim_dir", type=str, help="The simulation directory to calculate the IMs for"
    )
    parser.add_argument(
        "--n_proc",
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
        help="specify which verlocity compoent to calculate. choose from {}. Default is {}".format(
            IM_CALC_COMPONENTS, IM_CALC_COMPONENTS[0]
        ),
    )


    args = parser.parse_args()

