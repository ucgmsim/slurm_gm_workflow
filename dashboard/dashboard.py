#!/usr/bin/env python3
import os
import subprocess
import argparse
import getpass
from datetime import datetime
from qcore import utils

MAX_NODES = 264
CH_REPORT_PATH = '/nesi/project/nesi00213/workflow/scripts/CH_report.sh'
PROJECT_ID = 'nesi00213'
TIME_FORMAT = "%Y_%m_%d-%H_%M_%S"
HPCS = ['maui', 'mahuika']
OUT_DIR = os.path.join('/home', getpass.getuser(),'CH_usage')


def run_dashboard_cmds(user, hpc, period, out_dir):
    ssh_cmd = "ssh {}@{}".format(user, hpc)

    time = datetime.now().strftime(TIME_FORMAT)

    sub_outdir = os.path.join(out_dir,'{}_{}'.format(hpc, time))
    utils.setup_dir(sub_outdir)
    os.chdir(sub_outdir)

    his_ch_cmd = "{} bash {} {} >> {}_{}.txt".format(ssh_cmd, CH_REPORT_PATH, period, 'his_ch_usage', time)

    rt_ch_cmd = "{} nn_corehour_usage {} >> {}_{}.txt".format(ssh_cmd, PROJECT_ID, 'rt_ch_usage', time)

    rt_quota_cmd = "{} nn_check_quota >> {}_{}.txt".format(ssh_cmd, 'rt_quota_usage', time)

    sq_cmd = "{} squeue >> {}_{}.txt".format(ssh_cmd, 'squeue', time)

    capa_cmd = ssh_cmd + ' ' + "squeue -p nesi_research | awk '{print $10}'"

    subprocess.call(his_ch_cmd, shell=True)
    subprocess.call(rt_ch_cmd, shell=True)
    subprocess.call(rt_quota_cmd, shell=True)
    subprocess.call(sq_cmd, shell=True)
    output = subprocess.check_output(capa_cmd, shell=True).decode('utf-8').strip().split('\n')   # 'NODES\n23\n32\n'---> ['NODES', '23', '32']

    total_nodes = 0
    for i in range(len(output)):
        try:
            total_nodes += int(output[i])
        except ValueError:
            continue

    capcacity = (1 - total_nodes / MAX_NODES) * 100.

    s = "{}: Avavilable node capacity on partition nesi_research is {:.3f}%. {} nodes in use, Max nodes {}".format(hpc, capcacity, total_nodes, MAX_NODES)
    with open('capacity_{}.txt'.format(time), 'w') as f:
        f.write(s)
    print(s)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('user', default='melody.zhu',
                        help="hpc user name.eg. melody.zhu")
    parser.add_argument('period', default=2,
                        help="Please provide period(time interval) for CH_Report to run, default is 2")
    parser.add_argument('-o', '--out_dir', default=OUT_DIR,
                        help="Please provide out_dir for storing all core hour usage data")
    parser.add_argument('--hpc', choices=HPCS,
                        help="Please specify which hpc to log on, default both")

    args = parser.parse_args()
    utils.setup_dir(args.out_dir)

    if args.hpc is not None:
        run_dashboard_cmds(args.user, args.hpc, args.period, args.out_dir)
    else:
        for hpc in HPCS:
            run_dashboard_cmds(args.user, hpc, args.period, args.out_dir)


if __name__ == '__main__':
    main()
