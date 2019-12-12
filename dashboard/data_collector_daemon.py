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
from daemonize import Daemonize
from time import sleep
from os.path import exists,join,dirname,abspath

from typing import Iterable, List, Union
from datetime import datetime, date, timedelta
import logging

from qcore.shared import exe

import qcore.constants as const
from dashboard.DashboardDB import (
    DashboardDB,
    SQueueEntry,
    StatusEntry,
    QuotaEntry,
    UserChEntry,
    HPCProperty,
)

LOGDIR="/home/baes/dashboard_daemon/log"
LOCAL_DB="/home/baes/dashboard_daemon/dashboard.db"
REMOTE_DB="ec2-user@seistech.nz:/home/ec2-user/dashboard/dashboard.db"

MAX_NODES = 264
CH_REPORT_PATH = "/nesi/project/nesi00213/workflow/scripts/CH_report.sh"
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
    "ddempsey": "David Dempsey",
    "jagdish.vyas": "Jagdish Vyas",
}



pid = LOGDIR+"/data_collection.pid"

#logging.basicConfig(format='%(asctime)s: %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.propagate = False
fh = logging.FileHandler(LOGDIR+"/data_collection.log", "w")
fh.setLevel(logging.DEBUG)
fh.setFormatter(logging.Formatter('%(asctime)s: %(levelname)s: %(message)s'))
logger.addHandler(fh)
keep_fds = [fh.stream.fileno()]

def run_cmd(cmd: str, timeout: int = 180):
    """Runs the specified command remotely on the specified hpc using the
    specified user id.
    Returns False if the command fails for some reason.
    """
    logger.debug("EXEC: {}".format(cmd))
    try:
        out = subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True)
    except subprocess.CalledProcessError as e:
        logger.debug("ERR: {} {}".format(e.returncode, e.output.decode("utf-8")))
        return False
    else:
        out=out.decode("utf-8").strip()
        logger.debug("OUT: \n  {}".format(out))
        return out.split("\n")


class DataCollector:

    utc_time_format = "%m/%d/%y-%H:%M:%S"
    utc_time_gap = datetime.now() - datetime.utcnow()
    # core hour allocation renew date
    total_start_time = "01/06/{}-12:00:00".format(
        datetime.strftime(datetime.now() - timedelta(days=365), "%y")
    )

    def __init__(
        self,
        hpc: Union[List[const.HPC], const.HPC],
        users: List[str],
        project_id: str = const.DEFAULT_ACCOUNT,
        interval: int = 3600,
        error_th: int = 3,
    ):
        """
        Params
        ------
        hpc: HPC enum, or List of HPC enums
            The HPC clusters to collect data from
        interval: int
            How often to collect data
        error_th: int
            After how many executive errors to stop execution
        """
        if type(hpc) is const.HPC:
            self.hpc = [hpc]
            self.login_hpc = hpc.value
        else:  # if both hpc required, ssh to maui defautly and pull mahuika data from maui
            self.hpc = hpc
            self.login_hpc = "maui"
        # self.hpc = [hpc] if type(hpc) is const.HPC else hpc
        self.interval = interval
        dashboard_db = LOCAL_DB
        try:
            self.dashboard_db = DashboardDB(dashboard_db)
        except FileNotFoundError:
            self.dashboard_db = DashboardDB.create_db(dashboard_db)

        self.users = users
        self.project_id = project_id

        self.error_ctr = 0
        self.error_th = error_th

        self.ssh_cmd_template = "ssh {}@{} {}"

    def run(self):
        """Runs the data collection"""
        while True:
            # Collect the data
            logger.debug("{} - Collecting data from HPC {}".format(datetime.now(), self.hpc))
            for data_hpc in self.hpc:
                self.collect_data(data_hpc, self.users, self.project_id)
            logger.debug("{} - Done".format(datetime.now()))
            keyfile = join(dirname(LOCAL_DB),"aws_seistech_key.pem")
            logger.debug("Keyfile : {}".format(keyfile))
            out = run_cmd("scp -i {} {} {}".format(keyfile, LOCAL_DB, REMOTE_DB))
            if out:
                logger.debug("DB uploaded successfully")
            else:
                logger.error("DB upload failed")

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

    def collect_data(self, hpc: const.HPC, users: Iterable[str], project_id: str):
        """Collects data from the specified HPC and adds it to the
        dashboard db
        """
        start_time, end_time = self.get_utc_times()
        # Get daily core hours usage
        rt_daily_ch_output = run_cmd(
            "sreport -M {} -n -t Hours cluster AccountUtilizationByUser Accounts={} start={} end={} format=Cluster,Accounts,Login%30,Proper,Used".format(
                hpc.value, project_id, start_time, end_time
            )
        )
        if rt_daily_ch_output:
            print(rt_daily_ch_output)
            rt_daily_ch_output = self.parse_chours_usage(rt_daily_ch_output)

        rt_total_ch_output = run_cmd(
            "sreport -M {} -n -t Hours cluster AccountUtilizationByUser Accounts={} start={} end={} format=Cluster,Accounts,Login%30,Proper,Used".format(
                hpc.value, project_id, self.total_start_time, end_time
            )
        )
        if rt_total_ch_output:
            rt_total_ch_output = self.parse_chours_usage(rt_total_ch_output)
            self.dashboard_db.update_chours_usage(
                rt_daily_ch_output, rt_total_ch_output, hpc
            )

        # Squeue, formatted to show full account name
        sq_output = run_cmd(
            "squeue -M {} --format=%18i%25u%12a%60j%20T%25r%20S%18M%18L%10D%10C".format(
                hpc.value
            )
        )
        if sq_output:
            self.dashboard_db.update_squeue(self._parse_squeue(sq_output), hpc)

        # Node capacity
        # 'NODES\n23\n32\n'---> ['NODES', '23', '32']
        if hpc == const.HPC.maui:
            capa_output = run_cmd(
                "squeue -M {} -p nesi_research ".format(hpc.value)
                + "| awk '{print $10}'"
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
        quota_output = run_cmd(
            "nn_storage_quota {} | grep 'nesi00213'".format(hpc.value)
        )
        if quota_output:
            self.dashboard_db.update_daily_quota(self._parse_quota(quota_output), hpc)

        # user daily core hour usage
        user_ch_output = run_cmd(
            "sreport -M {} -t Hours cluster AccountUtilizationByUser Accounts={} Users={} start={} end={} -n format=Cluster,Account,Login%30,Proper,Used".format(
                hpc.value, project_id, " ".join(users), start_time, end_time
            )
        )
        if user_ch_output:
            self.dashboard_db.update_user_chours(
                hpc, self.parse_user_chours_usage(user_ch_output, users)
            )

    def _parse_squeue(self, lines: Iterable[str]):
        """Parse the results from the squeue command"""
        # lines
        # ['CLUSTER: maui',
        # 'JOBID  USER  ACCOUNT   NAME                 STATE   REASON START_TIME          TIME    TIME_LEFT NODES CPUS',
        # '505572 ykh22 nesi00213 sim_hf.MohakaS_REL27 RUNNING None   2019-07-30T10:45:22 1:59:48 14:12     2     160 ']
        entries = []
        for line in lines[2:]:  # skip headers
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
            except IndexError:
                logger.error(
                    "Failed to convert squeue line \n{}\n to "
                    "a valid SQueueEntry. Ignored!".format(line)
                )

        return entries

    @staticmethod
    def parse_chours_usage(ch_lines: List):
        """Get daily/total core hours usage from cmd output"""
        # ['maui', 'nesi00213', '2023', '0']
        try:
            return ch_lines[0].strip().split()[-1]
        # no core hours used, lines=['']
        except IndexError:
            return 0
        except ValueError:
            logger.error("Failed to convert total core hours to integer.")

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
                logger.error(
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
        #                                                       used
        # ['maui       nesi00213    jpa198 James Paterson+        1',
        #   maui       nesi00213     tdn27 Andrei Nguyen +      175']
        else:
            used_users = set()
            for ix, line in enumerate(lines):
                line = line.split()
                used_users.add(line[2])
                try:
                    entries.append(UserChEntry(day, line[2], line[-1]))
                except ValueError:
                    logger.error(
                        "Failed to convert user core hours usage line \n{}\n to "
                        "a valid UserChEntry. Ignored!".format(line)
                    )
            # get user that has usage 0
            unused_users = set(users) - used_users
            for user in unused_users:
                entries.append(UserChEntry(day, user, 0))
        return entries

def _start(args, debug=True):
    main(args)
    if debug:
        logger.debug(" ---- Started")
def _stop(args, debug=True):
    if exists(pid):
        cmd = "kill `cat {}`".format(pid)
        run_cmd(cmd)
        if debug:
            logger.debug(" ---- Stopped")

def _restart(args):
    _stop(args,debug=False)
    _start(args,debug=False)
    logger.debug(" ---- Restarted")

def main(args):
    hpc = (
        const.HPC(args.hpc)
        if type(args.hpc) is str
        else [const.HPC(cur_hpc) for cur_hpc in args.hpc]
    )
    data_col = DataCollector(hpc, args.users, args.project_id, args.update_interval)
#    data_col.run() # for easier debug, replace this line with the below.
    daemon = Daemonize(app="data_collection_app", pid=pid, action=data_col.run, keep_fds=keep_fds)
    daemon.start()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog='data_collection')

    sp = parser.add_subparsers()
    sp_start = sp.add_parser('start', help='Starts %(prog)s daemon')
    sp_stop = sp.add_parser('stop', help='Stops %(prog)s daemon')
    sp_restart = sp.add_parser('restart', help='Restarts %(prog)s daemon')

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

    parser.add_argument(
        "--project_id",
        type=str,
        help="Specify the project ID to collect old daily core hours usages for (back from today),"
        " default is {}".format(const.DEFAULT_ACCOUNT),
        default=const.DEFAULT_ACCOUNT,
    )

    sp_start.set_defaults(func=_start)
    sp_stop.set_defaults(func=_stop)
    sp_restart.set_defaults(func=_restart)

    args = parser.parse_args()
    args.func(args)

