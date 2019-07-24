#!/usr/bin/env python3
"""
Script for old dashboard data collection.
Stops running once finish collecting all the old core hour usage data by the specified collection period

The collected data populates the specified dashboard db.
"""

import argparse
import os
import subprocess
from typing import Iterable, List, Union
from datetime import datetime, timedelta

import qcore.constants as const
from dashboard.run_data_collection import HPCS, USERS, DataCollector
from dashboard.DashboardDB import DashboardDB

DEFAULT_DAYS_SHIFT = 406


def get_old_utc_times(day_shift: int):
    """Get the hpc utc time based on current real time - day_shift
       To be used as the start_time of sreport cmd
    """
    # 2019-03-25
    local_date = datetime.today().date()
    # if day_shift == 1: 2019-03-24 00:00:00
    local_start_datetime = datetime(
        local_date.year, local_date.month, local_date.day
    ) - timedelta(days=day_shift)
    utc_start_datetime = local_start_datetime - DataCollector.utc_time_gap
    start_utc_time_string = utc_start_datetime.strftime(DataCollector.utc_time_format)
    end_utc_time_string = (utc_start_datetime + timedelta(days=1)).strftime(
        DataCollector.utc_time_format
    )
    return local_start_datetime.date(), start_utc_time_string, end_utc_time_string


def collect_old_data(
    dashboard_db: DashboardDB,
    hpc: const.HPC,
    login_user: str,
    users: Iterable[str],
    day_shift: int,
):
    """Collect old core hours usage for a specified users in a previous day"""
    local_date, start_time, end_time = get_old_utc_times(day_shift)

    cmd = "sreport -M {} -t Hours cluster AccountUtilizationByUser Users={} start={} end={} -n format=Cluster,Accounts,Login%30,Proper,Used".format(
        hpc.value, " ".join(users), start_time, end_time
    )
    user_ch_output = (
        subprocess.check_output(
            "ssh {}@{} {}".format(login_user, hpc.value, cmd), shell=True, timeout=60
        )
        .decode("utf-8")
        .strip()
        .split("\n")
    )
    if user_ch_output:
        dashboard_db.update_user_chours(
            hpc,
            DataCollector.parse_user_chours_usage(user_ch_output, users, local_date),
            local_date,
        )
    cmd2 = "sreport -n -t Hours cluster AccountUtilizationByUser Accounts=nesi00213 start={} end={} format=Cluster,Accounts,Login%30,Proper,Used".format(
        start_time, end_time
    )

    rt_daily_ch_output = (
        subprocess.check_output(
            "ssh {}@{} {}".format(login_user, hpc.value, cmd2), shell=True, timeout=60
        )
        .decode("utf-8")
        .strip()
        .split("\n")
    )
    rt_daily_ch = DataCollector.parse_chours_usage(rt_daily_ch_output)

    # Get total core hours usage, start from 188 days ago
    cmd3 = "sreport -n -t Hours cluster AccountUtilizationByUser Accounts=nesi00213 start={} end={} format=Cluster,Accounts,Login%30,Proper,Used".format(
        DataCollector.total_start_time, end_time
    )

    rt_total_ch_output = (
        subprocess.check_output(
            "ssh {}@{} {}".format(login_user, hpc.value, cmd3), shell=True, timeout=60
        )
        .decode("utf-8")
        .strip()
        .split("\n")
    )
    rt_total_ch = DataCollector.parse_chours_usage(rt_total_ch_output)
    print("total", rt_total_ch)

    if rt_total_ch_output or rt_daily_ch_output:
        dashboard_db.update_chours_usage(rt_daily_ch, rt_total_ch, hpc, local_date)


def run_old_collection(
    dashboard_db,
    hpcs: Union[List[const.HPC], const.HPC],
    login_user: str,
    users: Iterable[str],
    days_shift: int,
):
    """Runs the data collection for a specified period"""
    # Iterate through the specifid days period
    for day_shift in reversed(range(days_shift + 1)):
        # Collect the data
        print("{} - Collecting data from HPC {}".format(datetime.now(), hpcs))
        for hpc in hpcs:
            collect_old_data(dashboard_db, hpc, login_user, users, day_shift)
            print("{} - Done".format(datetime.now()))


def main(args):
    hpc = (
        [const.HPC(args.hpc)]
        if type(args.hpc) is str
        else [const.HPC(cur_hpc) for cur_hpc in args.hpc]
    )
    if not os.path.isfile(args.dashboard_db):
        dashboard_db = DashboardDB.create_db(args.dashboard_db)
    else:
        dashboard_db = DashboardDB(args.dashboard_db)

    run_old_collection(dashboard_db, hpc, args.user, args.users, args.days_shift)


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
        help="Specify the HPC on which to collect data, defaults to all HPCs",
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
        " default is {}".format(DEFAULT_DAYS_SHIFT),
        default=DEFAULT_DAYS_SHIFT,
    )
    args = parser.parse_args()

    main(args)
