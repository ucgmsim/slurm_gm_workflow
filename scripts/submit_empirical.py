#!/usr/bin/env python

import argparse
from datetime import datetime
import os

from scripts.schedulers.scheduler_factory import initialise_scheduler, get_scheduler
from shared_workflow.platform_config import platform_config
from shared_workflow.shared_template import generate_context, resolve_header
from qcore import simulation_structure, utils
from qcore import constants as const


def rrup_file_exists(cybershake_folder, realisation):
    rrup_file = simulation_structure.get_rrup_path(cybershake_folder, realisation)
    return os.path.exists(rrup_file)


def write_sl(sl_name, content):
    fp = sl_name
    with open(fp, "w") as f:
        f.write(content)


def generate_sl(np, extended, cybershake_folder, realisations, out_dir):
    # extended is '-e' or ''

    faults = map(simulation_structure.get_fault_from_realisation, realisations)
    run_data = zip(realisations, faults)
    run_data = [
        (rel, fault)
        for (rel, fault) in run_data
        if rrup_file_exists(cybershake_folder, rel)
    ]
    # determine NP
    # TODO: empirical are currently not parallel, update this when they are
    np = 1
    # load sim_params for vs30_file
    # this is assuming all simulation use the same vs30 in root_params.yaml
    sim_dir = simulation_structure.get_sim_dir(cybershake_folder, run_data[0][0])
    sim_params = utils.load_sim_params(
        simulation_structure.get_sim_params_yaml_path(sim_dir)
    )

    timestamp_format = "%Y%m%d_%H%M%S"
    timestamp = datetime.now().strftime(timestamp_format)

    template_dir = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "..", "templates/"
    )

    header = resolve_header(
        platform_config[const.PLATFORM_CONFIG.TEMPLATES_DIR.name],
        wallclock_limit="00:30:00",
        job_name="empirical",
        version="slurm",
        memory="2G",
        exe_time="%j",
        job_description="Empirical Engine",
        additional_lines="",
        template_path=get_scheduler().HEADER_TEMPLATE,
        mail="",
        write_directory=out_dir,
    )
    context = generate_context(
        template_dir,
        "empirical.sl.template",
        {
            "run_data": run_data,
            "np": np,
            "extended": extended,
            "vs30_file": sim_params.stat_vs_est,
            "mgmt_db_location": cybershake_folder,
        },
    )
    sl_name = os.path.join(out_dir, "run_empirical_{}.sl".format(timestamp))
    content = "{}\n{}".format(header, context)
    write_sl(sl_name, content)
    return sl_name


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("cybershake_folder", help="Path to Cybershake root folder")
    parser.add_argument("-i", "--identifiers", nargs="+", help="realisation")
    parser.add_argument(
        "-e",
        "--extended_period",
        action="store_const",
        const="-e",
        default="",
        help="indicates extended pSA period to be calculated if present",
    )
    parser.add_argument(
        "-np", default=40, help="number of processes to use. Currently overridden to 1"
    )
    parser.add_argument(
        "--account",
        default=platform_config[const.PLATFORM_CONFIG.DEFAULT_ACCOUNT],
        help="specify the NeSI project",
    )
    parser.add_argument("-o", "--output_dir", type=os.path.abspath())

    args = parser.parse_args()

    initialise_scheduler("not_needed", account=args.account)

    generate_sl(
        args.np,
        args.extended_period,
        args.cybershake_folder,
        args.identifiers,
        args.output_dir,
    )


if __name__ == "__main__":
    main()
