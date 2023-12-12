#!/usr/bin/env python

import argparse
from datetime import datetime
import os
from pathlib import Path

from workflow.automation.lib.schedulers.scheduler_factory import Scheduler
from workflow.automation.platform_config import platform_config
from workflow.automation.lib.shared_template import generate_context, resolve_header
from qcore import simulation_structure, utils
from qcore import constants as const


def rrup_file_exists(cybershake_folder, realisation):
    rrup_file = simulation_structure.get_rrup_path(cybershake_folder, realisation)
    return os.path.exists(rrup_file)


def write_sl(sl_name, content):
    fp = sl_name
    with open(fp, "w") as f:
        f.write(content)


def generate_empirical_script(
    np, extended_switch, cybershake_folder, realisations, out_dir
):
    # extended_switch is '-e' or ''

    faults = map(simulation_structure.get_fault_from_realisation, realisations)
    run_data = zip(realisations, faults)
    run_data = [(rel, fault) for (rel, fault) in run_data]

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
        platform_config[const.PLATFORM_CONFIG.SCHEDULER_TEMPLATES_DIR.name],
        wallclock_limit="00:30:00",
        job_name="empirical",
        version="slurm",
        memory="2G",
        exe_time="%j",
        job_description="Empirical Engine",
        additional_lines="",
        template_path=platform_config[const.PLATFORM_CONFIG.HEADER_FILE.name],
        write_directory=out_dir,
        platform_specific_args={"n_tasks": np},
    )
    ll_ffp = sim_params["stat_file"]
    z_ffp = Path(ll_ffp).with_suffix(
        ".z"
    )  # .ll file and .z file are assumed to be at the same directory
    z_switch = (
        f"--z_ffp {z_ffp}" if z_ffp.exists() else ""
    )  #  empty z_switch -> z values to be estimated
    srf_ffp = sim_params["srf_file"]

    if sim_params.get("historical") == True:
        # If root_params.yaml has "historical : true", this will use NZ GMDB source for the event specific data
        srfinfo_switch = ""
    else:
        # this is a cybershake (future) event. We need srfinfo
        srfinfo_ffp = Path(srf_ffp).with_suffix(".info")
        assert srfinfo_ffp.exists(), "SRF info {srfinfo_ffp} not found"
        srfinfo_switch = f"--srfinfo_ffp {srfinfo_ffp}"

    context = generate_context(
        template_dir,
        "empirical.sl.template",
        {
            "np": np,
            "extended": extended,
            "run_data": run_data,
            "ll_ffp": ll_ffp,
            "vs30_ffp": sim_params["stat_vs_est"],
            "z_switch": z_switch,
            "srf_ffp": srf_ffp,
            "srfinfo_switch": srfinfo_switch,
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

    # The name parameter is only used to check user tasks in the queue monitor
    Scheduler.initialise_scheduler("", args.account)

    generate_empirical_script(
        args.np,
        args.extended_period,
        args.cybershake_folder,
        args.identifiers,
        args.output_dir,
    )


if __name__ == "__main__":
    main()
