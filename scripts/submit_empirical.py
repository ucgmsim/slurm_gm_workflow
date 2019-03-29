#!/usr/bin/env python

import argparse
from datetime import datetime
import os

from shared_workflow.shared import resolve_header, generate_context

DEFAULT_ACCOUNT = "nesi00213"


# TODO: Create library for this
def get_fault_name(run_name):
    return run_name.split("_")[0]


def rrup_file_exists(cybershake_folder, fault, realisation):
    rrup_file = os.path.join(
        cybershake_folder, "Runs/", fault, "verification/rrup_" + realisation + ".csv"
    )
    return os.path.exists(rrup_file)


def write_sl(sl_name, content):
    fp = sl_name
    with open(fp, "w") as f:
        f.write(content)


def generate_sl(np, extended, cybershake_folder, account, realisations):
    faults = map(get_fault_name, realisations)
    run_data = zip(realisations, faults)
    run_data = [
        (realisation, fault)
        for realisation, fault in run_data
        if rrup_file_exists(cybershake_folder, fault, realisation)
    ]
    timestamp_format = "%Y%m%d_%H%M%S"
    timestamp = datetime.now().strftime(timestamp_format)

    template_dir = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "..", "templates/"
    )

    header = resolve_header(
        account,
        np,
        wallclock_limit="00:30:00",
        job_name="empirical",
        version="slurm",
        memory="2G",
        exe_time="%j",
        job_description="Empirical Engine",
        mail='',
    )
    context = generate_context(
        template_dir,
        "empirical.sl.template",
        {
            "run_data": run_data,
            "np": np,
            "extended": extended,
            "mgmt_db_location": cybershake_folder,
        }
    )
    sl_name = "run_empirical_{}.sl".format(timestamp)
    content = "{}\n{}".format(header, context)
    write_sl(sl_name, content)


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

    args = parser.parse_args()

    generate_sl(
        args.np,
        args.extended_period,
        args.cybershake_folder,
        args.account,
        args.identifiers,
    )


if __name__ == "__main__":
    main()

