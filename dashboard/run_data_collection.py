#!/usr/bin/env python3
import time
import re
import subprocess
import argparse
from typing import Iterable

import qcore.constants as const
from dashboard.DashboardDB import DashboardDB, SQueueEntry, StatusEntry, HPCProperty

MAX_NODES = 264
CH_REPORT_PATH = "/nesi/project/nesi00213/workflow/scripts/CH_report.sh"
PROJECT_ID = "nesi00213"
TIME_FORMAT = "%Y_%m_%d-%H_%M_%S"
HPCS = [hpc.value for hpc in const.HPC]


def parse_total_chours_usage(lines: Iterable[str], hpc: const.HPC):
    """Gets the total core usage for the specified hpc
    from the output of the nn_corehour_usage command.

    If the output of nn_corehour_usage changes then this function
    will also have to be updated!
    """
    pattern = "Project .* on the {} cluster"
    hpc_pattern, cur_hpc_pattern = pattern.format(".*"), pattern.format(hpc.value)

    hpc_start_lines, cur_hpc_start_line = [], None

    for ix, line in enumerate(lines):
        if re.match(hpc_pattern, line):
            hpc_start_lines.append(ix)
            if re.match(cur_hpc_pattern, line):
                cur_hpc_start_line = ix

    # Get all lines between hpc of interest and next hpc entry (if there is one)
    if hpc_start_lines[-1] == cur_hpc_start_line:
        lines_interest = lines[cur_hpc_start_line:]
    else:
        lines_interest = lines[
            cur_hpc_start_line : hpc_start_lines[
                hpc_start_lines.index(cur_hpc_start_line) + 1
            ]
        ]

    # Get all lines starting with "Billed" then get the last one as it
    # shows the total core usage
    line_interest = [line for line in lines_interest if line.startswith("Billed")][-1]

    try:
        return int(line_interest.split(" ")[-4].strip().replace(",", ""))
    except ValueError:
        print("Failed to convert {} out of \n{}\n to integer.")
        return None


def parse_squeue(lines: Iterable[str]):
    """Parse the results from the squeue command"""
    entries = []
    for ix, line in enumerate(lines):
        if ix == 0:
            continue
        line = line.strip().split()

        try:
            entries.append(
                SQueueEntry(
                    line[0].strip(),
                    line[1].strip(),
                    line[4].strip(),
                    line[3].strip(),
                    line[7].strip(),
                    line[8].strip(),
                    int(line[9].strip()),
                    int(line[10].strip()),
                )
            )
        except ValueError:
            print(
                "Failed to convert squeue line \n{}\n to "
                "a valid SQueueEntry. Ignored!".format(line)
            )

    return entries

def collect_data(user: str, hpc: const.HPC, dashboard_db: str):
    """Collects data from the specified HPC and adds it to the
    dashboard db
    """
    hpc_value = hpc.value
    ssh_cmd = "ssh {}@{}".format(user, hpc_value)

    # Attempt to open existing database, otherwise create a new one
    try:
        dashboard_db = DashboardDB(dashboard_db)
    except FileNotFoundError:
        dashboard_db = DashboardDB.create_db(dashboard_db)

    # Get Core hour usage
    rt_ch_output = (
        subprocess.check_output(
            "{} nn_corehour_usage {}".format(ssh_cmd, PROJECT_ID), shell=True
        )
        .decode("utf-8")
        .strip()
        .split("\n")
    )

    # Update dashboard db
    dashboard_db.update_daily_chours_usage(
        parse_total_chours_usage(rt_ch_output, hpc), hpc
    )

    # Squeue
    sq_output = (
        subprocess.check_output("{} squeue".format(ssh_cmd), shell=True)
        .decode("utf-8")
        .strip()
        .split("\n")
    )

    # Update dashboard db
    dashboard_db.update_squeue(parse_squeue(sq_output), hpc)

    # 'NODES\n23\n32\n'---> ['NODES', '23', '32']
    if hpc == const.HPC.maui:
        capa_cmd = ssh_cmd + " " + "squeue -p nesi_research | awk '{print $10}'"
        output = (
            subprocess.check_output(capa_cmd, shell=True)
            .decode("utf-8")
            .strip()
            .split("\n")
        )

        total_nodes = 0
        for i in range(len(output)):
            try:
                total_nodes += int(output[i])
            except ValueError:
                continue

        dashboard_db.update_status_entry(const.HPC.maui,
                                         StatusEntry(HPCProperty.node_capacity.value,
                                                     HPCProperty.node_capacity.str_value,
                                                     total_nodes, MAX_NODES, None))


def main(args):
    while True:
        # Collect the data
        if type(args.hpc) is str:
            print("Collecting data from HPC {}".format(args.hpc))
            collect_data(args.user, const.HPC(args.hpc), args.dashboard_db)
        else:
            print("Collecting data from HPC {}".format(HPCS))
            for hpc in HPCS:
                collect_data(args.user, const.HPC(hpc), args.dashboard_db)

        time.sleep(args.update_interval)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "user",
        help="Username to run the data collection under, needs to have an open ssh socket to the HPC(s)",
    )
    parser.add_argument("dashboard_db", help="The dashboard db to use")
    parser.add_argument(
        "--update_interval",
        help="Interval between data collection (seconds)",
        default=30,
    )
    parser.add_argument(
        "--hpc",
        choices=HPCS,
        help="Specify the HPCs on which to collect data, defaults to all HPCs",
        default=HPCS,
    )
    args = parser.parse_args()

    main(args)
