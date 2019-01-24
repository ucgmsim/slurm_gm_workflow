import os
import subprocess
from crontab import CronTab
from datetime import datetime
from qcore import utils

user = 'melody.zhu'

hpc = 'maui'

ch_report_path = '/nesi/project/nesi00213/workflow/scripts/CH_report.sh'

out_dir = '/home/yzh231/CH_usage'

period = 2


PROJECT_ID = 'nesi00213'
SSH_CMD = "ssh {}@{}".format(user, hpc)
TIME_FORMAT = "%Y_%m_%d-%H_%M_%S"
TIME = datetime.now().strftime(TIME_FORMAT)

HIS_CH_CMD = "{} bash {} {} >> {}_{}.txt".format(SSH_CMD, ch_report_path, period, 'his_ch_usage', TIME)

RT_CH_CMD = "{} nn_corehour_usage {} >> {}_{}.txt".format(SSH_CMD, PROJECT_ID, 'rt_ch_usage', TIME)

RT_QUOTA_CMD = "{} nn_check_quota >> {}_{}.txt".format(SSH_CMD, 'rt_quota_usage', TIME)

SQ_CMD = "{} squeue >> {}_{}.txt".format(SSH_CMD, 'squeue', TIME)

# subprocess.call(HIS_CH_CMD, shell=True, encoding='utf-8')
# subprocess.call(RT_CH_CMD, shell=True, encoding='utf-8')
# subprocess.call(RT_QUOTA_CMD, shell=True, encoding='utf-8')
subprocess.call(SQ_CMD, shell=True, encoding='utf-8')