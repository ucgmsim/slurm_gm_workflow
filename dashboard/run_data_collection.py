#!/usr/bin/env python3
"""
Script for dashboard data collection. Runs an never-ending with a sleep between
data collection. If any of the ssh commands fail, then the script stops, to prevent
HPC lockout of the user running this script.

The collected data populates the specified dashboard db.
"""

import time
import re
import subprocess
import argparse
from typing import Iterable, List, Union
from datetime import datetime

import qcore.constants as const
from dashboard.DashboardDB import DashboardDB, SQueueEntry, StatusEntry, QuotaEntry, HPCProperty

MAX_NODES = 264
CH_REPORT_PATH = "/nesi/project/nesi00213/workflow/scripts/CH_report.sh"
PROJECT_ID = "nesi00213"
TIME_FORMAT = "%Y_%m_%d-%H_%M_%S"
HPCS = [hpc.value for hpc in const.HPC]


class DataCollector:
    def __init__(
        self,
        user: str,
        hpc: Union[List[const.HPC], const.HPC],
        dashboard_db: str,
        interval: int = 30,
        error_th: int = 3,
    ):
        """
        Params
        ------
        user: str,
            Username used for executing commands on HPC
        hpc: HPC enum, or List of HPC enums
            The HPC clusters to collect data from
        dashboard_db: str
            Filepath for the db to save data to
        interval: int
            How often to collect data
        error_th: int
            After how many executive errors to stop execution
        """
        self.user = user
        self.hpc = [hpc] if type(hpc) is const.HPC else hpc
        self.interval = interval

        try:
            self.dashboard_db = DashboardDB(dashboard_db)
        except FileNotFoundError:
            self.dashboard_db = DashboardDB.create_db(dashboard_db)

        self.error_ctr = 0
        self.error_th = error_th

        self.ssh_cmd_template = "ssh {}@{} {}"

    def run(self):
        """Runs the data collection"""
        while True:
            # Collect the data
            print("{} - Collecting data from HPC {}".format(datetime.now(), self.hpc))
            for hpc in self.hpc:
                self.collect_data(hpc)
            print("{} - Done".format(datetime.now()))

            time.sleep(self.interval)

    def collect_data(self, hpc: const.HPC):
        """Collects data from the specified HPC and adds it to the
        dashboard db
        """
        # Get Core hour usage, out of some reason this command is super slow...
        rt_ch_output = self.run_cmd(
            hpc.value, "nn_corehour_usage {}".format(PROJECT_ID), timeout=60
        )
        if rt_ch_output:
            self.dashboard_db.update_daily_chours_usage(
                self._parse_total_chours_usage(rt_ch_output, hpc), hpc
            )

        # Squeue
        sq_output = self.run_cmd(self.user, hpc.value, "squeue")
        if sq_output:
            self.dashboard_db.update_squeue(self._parse_squeue(sq_output), hpc)

        # Node capacity
        # 'NODES\n23\n32\n'---> ['NODES', '23', '32']
        if hpc == const.HPC.maui:
            capa_output = self.run_cmd(
                hpc.value, "squeue -p nesi_research | awk '{print $10}'"
            )

            if capa_output:
                total_nodes = 0
                for line in capa_output:
                    try:
                        total_nodes += int(line)
                    except ValueError:
                        continue

                self.dashboard_db.update_status_entry(
                    const.HPC.maui,
                    StatusEntry(
                        HPCProperty.node_capacity.value,
                        HPCProperty.node_capacity.str_value,
                        total_nodes,
                        MAX_NODES,
                        None,
                    ),
                )
        # get quota
        quota_output = self.run_cmd(
            hpc.value, "nn_check_quota | grep 'nesi00213'", timeout=60
        )
        if quota_output:
            self.dashboard_db.update_daily_quota(self._parse_quota(quota_output), hpc)

    def run_cmd(self, hpc: str, cmd: str, timeout: int = 10):
        """Runs the specified command remotely on the specified hpc using the
        specified user id.

        Returns False if the command fails for some reason.
        """
        print(self.ssh_cmd_template.format(self.user, hpc, cmd))
        try:
            result = (
                subprocess.check_output(
                    self.ssh_cmd_template.format(self.user, hpc, cmd),
                    shell=True,
                    timeout=timeout,
                )
                .decode("utf-8")
                .strip()
                .split("\n")
            )
        except subprocess.CalledProcessError:
            print("Cmd {} returned a non-zero exit status.".format(cmd))
            self.error_ctr += 1
        except subprocess.TimeoutExpired:
            print("The timeout of {} expired for cmd {}.".format(timeout, cmd))
            self.error_ctr += 1
        else:
            self.error_ctr = 0
            return result

        # Check that everything went well
        if self.error_th <= self.error_ctr:
            raise Exception(
                "There have been {} consecutive collection cmd failures".format(
                    self.error_th
                )
            )

    def _parse_squeue(self, lines: Iterable[str]):
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

    def _parse_total_chours_usage(self, lines: Iterable[str], hpc: const.HPC):
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
        line_interest = [line for line in lines_interest if line.startswith("Billed")][
            -1
        ]

        try:
            return int(line_interest.split(" ")[-4].strip().replace(",", ""))
        except ValueError:
            print("Failed to convert {} out of \n{}\n to integer.")
            return None

    def _parse_quota(self, lines: Iterable[str]):
        entries = []
        for ix, line in enumerate(lines):
            line = line.split()
            if ix == 0:

            try:
                entries.append(
                    QuotaEntry(
                        line[0].strip(),
                        line[2].strip(),
                        int(line[4].strip()),
                        int(line[5].strip()),
                    )
                )
            except ValueError:
                print(
                    "Failed to convert squeue line \n{}\n to "
                    "a valid SQueueEntry. Ignored!".format(line)
                )

        return entries


def main(args):
    hpc = (
        const.HPC(args.hpc)
        if type(args.hpc) is str
        else [const.HPC(cur_hpc) for cur_hpc in args.hpc]
    )
    data_col = DataCollector(args.user, hpc, args.dashboard_db, args.update_interval)
    data_col.run()


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
