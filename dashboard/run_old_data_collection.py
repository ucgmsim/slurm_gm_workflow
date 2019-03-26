#!/usr/bin/env python3
"""
Script for old dashboard data collection.
Stops running once finish collecting all the old core hour usage data by the specified collection period

The collected data populates the specified dashboard db.
"""

import argparse
from typing import Iterable, List, Union
from datetime import datetime, timedelta

import qcore.constants as const
from dashboard.run_data_collection import HPCS, USERS, DataCollector

DAYS_SHIFT = 10


class OldDataCollector(DataCollector):
    def __init__(
        self,
        user: str,
        hpc: Union[List[const.HPC], const.HPC],
        dashboard_db: str,
        days_shift: int,
        interval: int = 30,
        error_th: int = 3,
    ):
        """
        Sub class of DataCollector
        :param days_shift: total days in the past from today to collect old core hours usage.
        """
        super().__init__(user, hpc, dashboard_db, interval, error_th)
        self.days_shift = days_shift

    def get_old_utc_times(self, day_shift):
        """Get the hpc utc time based on current real time - day_shift
           To be used as the start_time of sreport cmd
        """
        # 2019-0
        local_date = datetime.today().date()
        # 2019-03-25 00:00:00
        local_start_datetime = datetime(local_date.year, local_date.month, local_date.day) - timedelta(days=day_shift)
        # if day_shift == 1: 2019-03-24 00:00:00
        local_date_2 = local_start_datetime.date()
        utc_start_datetime = local_start_datetime - self.utc_time_gap
        start_utc_time_string = utc_start_datetime.strftime(self.utc_time_format)
        end_utc_datetime = utc_start_datetime + timedelta(days=1)
        end_utc_time_string = end_utc_datetime.strftime(self.utc_time_format)

        return local_date_2, start_utc_time_string, end_utc_time_string

    def run_old(self, users, days_shift):
        """Runs the data collection for a specified period"""
        # Iterate through the specifid days period
        for day_shift in range(days_shift + 1):
            # Collect the data
            print("{} - Collecting data from HPC {}".format(datetime.now(), self.hpc))
            for hpc in self.hpc:
                self.collect_old_data(hpc, users, day_shift)
            print("{} - Done".format(datetime.now()))

    def collect_old_data(self, hpc: const.HPC, users: Iterable[str], day_shift: int):
        """Collect old core hours usage for a specified users in a previous day"""
        local_date, start_time, end_time = self.get_old_utc_times(day_shift)
        user_ch_output = self.run_cmd(
            hpc.value,
            "sreport -M {} -t Hours cluster AccountUtilizationByUser Users={} start={} end={} -n".format(
                hpc.value, " ".join(users), start_time, end_time
            ),
        )
        if user_ch_output:
            self.dashboard_db.update_user_chours(
                hpc,
                self._parse_user_chours_usage(user_ch_output, users, local_date),
                local_date,
            )


def main(args):
    hpc = (
        const.HPC(args.hpc)
        if type(args.hpc) is str
        else [const.HPC(cur_hpc) for cur_hpc in args.hpc]
    )

    data_col = OldDataCollector(
        args.user, hpc, args.dashboard_db, args.days_shift, args.update_interval
    )
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
        "--days_shift",
        type=int,
        help="Specify the days to collect old daily core hours usages for (back from today),"
        " default is {}".format(DAYS_SHIFT),
        default=DAYS_SHIFT,
    )
    args = parser.parse_args()

    main(args)
