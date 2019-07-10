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
from datetime import datetime, date, timedelta

import qcore.constants as const
from dashboard.DashboardDB import (
    DashboardDB,
    SQueueEntry,
    StatusEntry,
    QuotaEntry,
    UserChEntry,
    HPCProperty,
)

MAX_NODES = 264
CH_REPORT_PATH = "/nesi/project/nesi00213/workflow/scripts/CH_report.sh"
PROJECT_ID = "nesi00213"
TIME_FORMAT = "%Y_%m_%d-%H_%M_%S"
HPCS = [hpc.value for hpc in const.HPC]
USERS = {
    "ykh22": "Jonney Huang",
    "cbs51": "Claudio Schill",
    "melody.z+": "Melody Zhu",
    "tdn27": "Andrei Nguyen",
    "jpa198": "James Paterson",
    "leer": "Robin Lee",
    "baes": "Sung Bae",
    "sjn87": "Sarah Neill",
    "jmotha": "Jason Motha",
    "ddempsey": "David Dempsey",
}


class DataCollector:

    utc_time_format = "%m/%d/%y-%H:%M:%S"
    utc_time_gap = datetime.now() - datetime.utcnow()
    # core hour allocation renew date
    total_start_time = "01/06/{}-12:00:00".format(
        datetime.strftime(datetime.now() - timedelta(days=365), "%y")
    )

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

    def run(self, users):
        """Runs the data collection"""
        while True:
            # Collect the data
            print("{} - Collecting data from HPC {}".format(datetime.now(), self.hpc))
            for hpc in self.hpc:
                self.collect_data(hpc, users)
            print("{} - Done".format(datetime.now()))

            time.sleep(self.interval)

    def get_utc_times(self):
        """Get the hpc utc time based on current real time
           To be used as the start_time of sreport cmd
        """
        # 2019-03-26
        local_date = datetime.today().date()
        # 2019-03-26 00:00:00
        local_start_datetime = datetime(
            local_date.year, local_date.month, local_date.day
        )
        # 2019-03-25 11:00:00.000002
        utc_start_datetime = local_start_datetime - self.utc_time_gap
        # 03/25/19-11:00:00
        start_utc_time_string = utc_start_datetime.strftime(self.utc_time_format)
        # 2019-03-26 11:00:00.000002
        end_utc_datetime = utc_start_datetime + timedelta(days=1)
        # 03/26/19-11:00:00
        end_utc_time_string = end_utc_datetime.strftime(self.utc_time_format)

        return start_utc_time_string, end_utc_time_string

    def collect_data(self, hpc: const.HPC, users: Iterable[str]):
        """Collects data from the specified HPC and adds it to the
        dashboard db
        """
        start_time, end_time = self.get_utc_times()
        # Get daily core hours usage
        rt_daily_ch_output = self.run_cmd(
            hpc.value,
            "sreport -n -t Hours cluster AccountUtilizationByUser Accounts={} start={} end={}".format(
                PROJECT_ID, start_time, end_time
            ),
        )
        if rt_daily_ch_output:
            rt_daily_ch_output = self.parse_chours_usage(rt_daily_ch_output)

        rt_total_ch_output = self.run_cmd(
            hpc.value,
            "sreport -n -t Hours cluster AccountUtilizationByUser Accounts={} start={} end={}".format(
                PROJECT_ID, self.total_start_time, end_time
            ),
        )
        if rt_total_ch_output:
            rt_total_ch_output = self.parse_chours_usage(rt_total_ch_output)
            self.dashboard_db.update_chours_usage(rt_daily_ch_output, rt_total_ch_output, hpc)

        # Squeue, formatted to show full account name
        sq_output = self.run_cmd(
            hpc.value, "squeue --format=%18i%25u%12a%60j%20T%25r%20S%18M%18L%10D%10C"
        )
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
        quota_output = self.run_cmd(hpc.value, "nn_check_quota | grep 'nesi00213'")
        if quota_output:
            self.dashboard_db.update_daily_quota(self._parse_quota(quota_output), hpc)

        # user daily core hour usage
        user_ch_output = self.run_cmd(
            hpc.value,
            "sreport -M {} -t Hours cluster AccountUtilizationByUser Users={} start={} end={} -n".format(
                hpc.value, " ".join(users), start_time, end_time
            ),
        )
        if user_ch_output:
            self.dashboard_db.update_user_chours(
                hpc, self.parse_user_chours_usage(user_ch_output, users)
            )

    def run_cmd(self, hpc: str, cmd: str, timeout: int = 180):
        """Runs the specified command remotely on the specified hpc using the
        sbaespecified user id.
        Returns False if the command fails for some reason.
        """
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
                        line[2].strip(),
                        line[4].strip(),
                        line[3].strip(),
                        line[7].strip(),
                        line[8].strip(),
                        # some usename has space, which increases the length of line after splitting
                        int(line[-2].strip()),
                        int(line[-1].strip()),
                    )
                )
            except ValueError:
                print(
                    "Failed to convert squeue line \n{}\n to "
                    "a valid SQueueEntry. Ignored!".format(line)
                )

        return entries

    @staticmethod
    def parse_chours_usage(ch_lines: List):
        """Get daily/total core hours usage from cmd output"""
        # ['maui', 'nesi00213', '2023', '0']
        try:
            return ch_lines[0].strip().split()[-2]
        # no core hours used, lines=['']
        except IndexError:
            return 0
        except ValueError:
            print("Failed to convert total core hours to integer.")

    def _parse_quota(self, lines: Iterable[str]):
        """
        Gets quota usage from cmd and return as a list of QuotaEntry objects
        :param lines: output from cmd to get quota usage
        """
        # lines:
        #                               used              Inodes     Iused
        # ['project_nesi00213'  '1T'  '98.16G'  '9.59%'  '1000000'  '277361'  '27.74%',
        #                          used      Inodes        Iused
        #  'nobackup_nesi00213'  '84.59T'  '15000000'  '10183142'  '67.89%']
        entries = []
        for ix, line in enumerate(lines):
            line = line.split()
            try:
                if len(line) == 7:  # first line
                    entries.append(
                        QuotaEntry(
                            line[0].strip(),
                            line[2].strip(),
                            int(line[4].strip()),
                            int(line[5].strip()),
                            date.today(),
                        )
                    )
                elif len(line) == 5:
                    entries.append(
                        QuotaEntry(
                            line[0].strip(),
                            line[1].strip(),
                            int(line[2].strip()),
                            int(line[3].strip()),
                            date.today(),
                        )
                    )
            except ValueError:
                print(
                    "Failed to convert quota usage line \n{}\n to "
                    "a valid QuotaEntry. Ignored!".format(line)
                )

        return entries

    @staticmethod
    def parse_user_chours_usage(
        lines: Iterable[str], users: Iterable[str], day: Union[date, str] = date.today()
    ):
        """Get daily core hours usage for a list of users from cmd output"""
        entries = []
        # if none of the user had usage today, lines=['']
        # Then we just set core_hour_used to 0 for all users
        if lines == [""]:
            for user in users:
                entries.append(UserChEntry(day, user, 0))
        # if some of the users had usages today
        #                                                        used
        # ['maui       nesi00213    jpa198 James Paterson+        1        0 ',
        #   maui       nesi00213     tdn27 Andrei Nguyen +      175        0']
        else:
            used_users = set()
            for ix, line in enumerate(lines):
                line = line.split()
                used_users.add(line[2])
                try:
                    entries.append(UserChEntry(day, line[2], line[-2]))
                except ValueError:
                    print(
                        "Failed to convert user core hours usage line \n{}\n to "
                        "a valid UserChEntry. Ignored!".format(line)
                    )
            # get user that has usage 0
            unused_users = set(users) - used_users
            for user in unused_users:
                entries.append(UserChEntry(day, user, 0))
        return entries


def main(args):
    hpc = (
        const.HPC(args.hpc)
        if type(args.hpc) is str
        else [const.HPC(cur_hpc) for cur_hpc in args.hpc]
    )
    data_col = DataCollector(args.user, hpc, args.dashboard_db, args.update_interval)
    data_col.run(args.users)


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
        default=300,
    )
    parser.add_argument(
        "--hpc",
        choices=HPCS,
        help="Specify the HPCs on which to collect data, defaults to all HPCs",
        default=HPCS,
    )

    parser.add_argument(
        "--users",
        help="Specify the users to collect daily core hours usages for, default is {}".format(
            USERS.keys()
        ),
        default=USERS.keys(),
    )
    args = parser.parse_args()

    main(args)
