#!/usr/bin/env python3
import os
import re
import subprocess
import argparse
import getpass
from datetime import datetime
from typing import Iterable

import qcore.constants as const
from qcore import utils
from dashboard.DashboardDB import DashboardDB, SQueueEntry

MAX_NODES = 264
CH_REPORT_PATH = "/nesi/project/nesi00213/workflow/scripts/CH_report.sh"
PROJECT_ID = "nesi00213"
TIME_FORMAT = "%Y_%m_%d-%H_%M_%S"
HPCS = [hpc.value for hpc in const.HPC]
OUT_DIR = os.path.join("/home", getpass.getuser(), "CH_usage")
DASHBOARD_DB_FILE = "dashboard.db"


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
    entries = []
    for ix, line in enumerate(lines):
        if ix == 0:
            continue
        line = line.strip().split()

        try:
            entries.append(SQueueEntry(
                line[0].strip(),
                line[1].strip(),
                line[4].strip(),
                line[3].strip(),
                line[7].strip(),
                line[8].strip(),
                int(line[9].strip()),
                int(line[10].strip()),
            ))
        except ValueError:
            print("Failed to convert squeue line \n{}\n to "
                  "a valid SQueueEntry. Ignored!".format(line))

    return entries


def run_dashboard_cmds(
    user: str, hpc: const.HPC, period: float, out_dir: str, dashboard_db: str
):
    def run_and_write(cmd: str, out_file: str):
        output = (
            subprocess.check_output(cmd, shell=True).decode("utf-8").strip().split("\n")
        )

        # Write to file
        with open(out_file, "w") as f:
            f.writelines(output)

        return output

    hpc_value = hpc.value
    ssh_cmd = "ssh {}@{}".format(user, hpc_value)

    time = datetime.now().strftime(TIME_FORMAT)

    sub_outdir = os.path.join(out_dir, "{}_{}".format(hpc_value, time))
    utils.setup_dir(sub_outdir)
    os.chdir(sub_outdir)

    # Attempt to open existing database, otherwise create a new one
    try:
        dashboard_db = DashboardDB(dashboard_db)
    except FileNotFoundError:
        dashboard_db = DashboardDB.create_db(dashboard_db)

    # Get Core hour usage
    rt_ch_cmd = "{} nn_corehour_usage {}".format(ssh_cmd, PROJECT_ID)
    rt_ch_out_file = "{}_{}.txt".format("rt_ch_usage", time)
    rt_ch_output = run_and_write(rt_ch_cmd, rt_ch_out_file)

    # Update dashboard db
    dashboard_db.update_daily_chours_usage(
        parse_total_chours_usage(rt_ch_output, hpc), hpc
    )

    # Squeue
    sq_out_file = "squeue_{}.txt".format(time)
    sq_cmd = "{} squeue -A nesi00213".format(ssh_cmd)
    sq_output = run_and_write(sq_cmd, sq_out_file)

    # Update dashboard db
    dashboard_db.update_squeue(parse_squeue(sq_output), hpc)


    # Other commands, not saved in dashboard db
    his_ch_cmd = "{} bash {} {} >> {}_{}.txt".format(
        ssh_cmd, CH_REPORT_PATH, period, "his_ch_usage", time
    )

    rt_quota_cmd = "{} nn_check_quota >> {}_{}.txt".format(
        ssh_cmd, "rt_quota_usage", time
    )

    capa_cmd = ssh_cmd + " " + "squeue -p nesi_research | awk '{print $10}'"

    subprocess.call(his_ch_cmd, shell=True)
    subprocess.call(rt_quota_cmd, shell=True)

    # 'NODES\n23\n32\n'---> ['NODES', '23', '32']
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

    capcacity = (1 - total_nodes / MAX_NODES) * 100.0

    s = "{}: Avavilable node capacity on partition nesi_research is {:.3f}%. {} nodes in use, Max nodes {}".format(
        hpc_value, capcacity, total_nodes, MAX_NODES
    )
    with open("capacity_{}.txt".format(time), "w") as f:
        f.write(s)
    print(s)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "user", default="melody.zhu", help="hpc user name.eg. melody.zhu"
    )
    parser.add_argument(
        "--period",
        default=2,
        help="Please provide period(time interval) for CH_Report to run, default is 2",
    )
    parser.add_argument(
        "-o",
        "--out_dir",
        default=OUT_DIR,
        help="Please provide out_dir for storing all core hour usage data",
    )
    parser.add_argument(
        "--hpc", choices=HPCS, help="Please specify which hpc to log on, default both"
    )
    parser.add_argument(
        "--dashboard_db",
        default=os.path.join(OUT_DIR, DASHBOARD_DB_FILE),
        help="The dashboard db to use, default is {}".format(
            os.path.join(OUT_DIR, DASHBOARD_DB_FILE)
        ),
    )

    args = parser.parse_args()
    utils.setup_dir(args.out_dir)

    if args.hpc is not None:
        run_dashboard_cmds(
            args.user, const.HPC(args.hpc), args.period, args.out_dir, args.dashboard_db
        )
    else:
        for hpc in HPCS:
            run_dashboard_cmds(
                args.user, const.HPC(hpc), args.period, args.out_dir, args.dashboard_db
            )


if __name__ == "__main__":
    main()
