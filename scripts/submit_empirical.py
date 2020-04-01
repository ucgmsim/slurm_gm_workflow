#!/usr/bin/env python

import argparse
from datetime import datetime
import os

from shared_workflow.shared_defaults import recipe_dir
from shared_workflow.shared_template import generate_context, resolve_header
from qcore.simulation_structure import get_rrup_path

DEFAULT_ACCOUNT = "nesi00213"


# TODO: Create library for this
def get_fault_name(run_name):
    return run_name.split("_")[0]


def rrup_file_exists(cybershake_folder, fault, realisation):
    rrup_file = get_rrup_path(cybershake_folder, realisation)
    return os.path.exists(rrup_file)


def write_sl(sl_name, content):
    fp = sl_name
    with open(fp, "w") as f:
        f.write(content)


def generate_sl(np, extended, cybershake_folder, account, realisations, out_dir):
    # extended is '-e' or ''

    faults = map(get_fault_name, realisations)
    run_data = zip(realisations, faults)
    run_data = [
        (rel, fault)
        for (rel, fault) in run_data
        if rrup_file_exists(cybershake_folder, fault, rel)
    ]

    timestamp_format = "%Y%m%d_%H%M%S"
    timestamp = datetime.now().strftime(timestamp_format)

    template_dir = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "..", "templates/"
    )

    header = resolve_header(
        recipe_dir,
        account,
        np,
        wallclock_limit="00:30:00",
        job_name="empirical",
        version="slurm",
        memory="2G",
        exe_time="%j",
        job_description="Empirical Engine",
        mail="",
        partition=None,
        additional_lines="",
        template_path="slurm_header.cfg",
        write_directory=out_dir,
    )
    context = generate_context(
        template_dir,
        "empirical.sl.template",
        {
            "run_data": run_data,
            "np": np,
            "extended": extended,
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
    parser.add_argument("-np", default=40, help="number of processes to use")
    parser.add_argument(
        "--account", default=DEFAULT_ACCOUNT, help="specify the NeSI project"
    )
    parser.add_argument("-o", "--output_dir", type=os.path.abspath())

    args = parser.parse_args()

    generate_sl(
        args.np,
        args.extended_period,
        args.cybershake_folder,
        args.account,
        args.identifiers,
        args.output_dir,
    )


if __name__ == "__main__":
    main()
