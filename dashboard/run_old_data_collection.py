import time
import re
import subprocess
import argparse
from typing import Iterable, List, Union
from datetime import datetime, date, timedelta

import qcore.constants as const
from dashboard.run_data_collection import DataCollector
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
    "melody.zhu": "Melody Zhu",
    "tdn27": "Andrei Nguyen",
    "jpa198": "James Paterson",
    "leer": "Robin Lee",
    "baes": "Sung Bae",
    "sjn87": "Sarah Neill",
    "jmotha": "Jason Motha",
    "Jagdish.vyas": "jagdish Vyas",
    "ddempsey": "David Dempsey",
}


class OldDataCollector(DataCollector):
    def __init__(self, user: str, hpc: Union[List[const.HPC], const.HPC], dashboard_db: str,  days_shift: int, interval: int=30, error_th: int=3):
        super().__init__(user, hpc, dashboard_db, interval, error_th)
        self.days_shift = days_shift

    def get_old_utc_times(self, day_shift):
        """Get the hpc utc time based on current real time
           To be used as the start_time of sreport cmd
        """
        print(day_shift)
        # 2019-03-26
        local_date = datetime.today().date()
        print("local_date 1", local_date)
        # 2019-03-25 00:00:00
        local_start_datetime = datetime(local_date.year, local_date.month, local_date.day) - timedelta(days=day_shift)
        print("local_stat_datetime", local_start_datetime)
        local_date_2 = local_start_datetime.date()
        print("local_date2", local_date)
        utc_start_datetime = local_start_datetime - self.utc_time_gap
        print("utc_stat_datetime", utc_start_datetime)
        start_utc_time_string = utc_start_datetime.strftime(self.utc_time_format)
        end_utc_datetime = utc_start_datetime + timedelta(days=1)
        print("end_utc_datetime", end_utc_datetime)
        end_utc_time_string = end_utc_datetime.strftime(self.utc_time_format)

        return local_date_2, start_utc_time_string, end_utc_time_string

    def run_old(self, users, days_shift):
        """Runs the data collection"""
        for day_shift in range(days_shift + 1):
            # Collect the data
            print("{} - Collecting data from HPC {}".format(datetime.now(), self.hpc))
            for hpc in self.hpc:
                self.collect_old_data(hpc, users, day_shift)
            print("{} - Done".format(datetime.now()))

            #time.sleep(self.interval)

    def collect_old_data(self, hpc: const.HPC, users: Iterable[str], day_shift: int):
        local_date, start_time, end_time = self.get_old_utc_times(day_shift)
        print("local_date, start_time, end_time", local_date, start_time, end_time)
        user_ch_output = self.run_cmd(
            hpc.value,
            "sreport -M {} -t Hours cluster AccountUtilizationByUser Users={} start={} end={} -n".format(
                hpc.value, " ".join(users), start_time, end_time
            ),
        )
        if user_ch_output:
            self.dashboard_db.update_user_chours(
                hpc, self._parse_old_user_chours_usage(user_ch_output, users, local_date), local_date
            )

    def _parse_old_user_chours_usage(self, lines: Iterable[str], users: Iterable[str], day: Union[date, str]):
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

    data_col = OldDataCollector(args.user, hpc, args.dashboard_db, args.days_shift, args.update_interval)
    data_col.run_old(args.users, args.days_shift)



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

    parser.add_argument(
        "--users",
        help="Specify the users to collect daily core hours usages for, default is {}".format(
            USERS.keys()
        ),
        default=USERS.keys(),
    )

    parser.add_argument(
        "--days_shift", type=int,
        help="Specify the days to collect old daily core hours usages for (back from today), default is 365",
        default=365,
    )
    args = parser.parse_args()

    main(args)
