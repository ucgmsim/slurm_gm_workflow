#!/usr/bin/env python3

import sys
import json
import subprocess
import argparse
import getpass
from enum import Enum

TEST_CMD = 'mkdir test_sbatch'
MB_PER_GB = 1024.

DEFAULT_NTASKS = 1
DEFAULT_CPUS_PER_TASK = 1

MAUI_PARTITIONS= ['nesi_research']
MAHUIKA_PARTITIONS = ["large", "long", "prepost", "bigmem", "hugemem"]


class SlurmHeader(Enum):
    ACCOUNT = "account"
    PARTITION = "partition"
    NTASKS = "ntasks"
    CPUS_PER_TASK = "cpus-per-task"
    MEM_PER_CPU = "mem-per-cpu"


def get_billing_weights(line):
    # ["'CPU=1.0,Mem=0.1429G'", 'QOS=p_prepost2', 'MaxTime=03:00:00', 'PriorityTier=1']
    cpu, mem = line.strip().split("TRESBillingWeights=")[-1].split()[0].split(',')
    cpu = float(cpu.split('=')[-1])
    mem = float(mem.split('=')[-1][:-2])
    print(cpu,mem)
    return cpu, mem


def get_default_mem_per_cpu(slurm_conf):
    with open(slurm_conf) as f:
        lines = f.readlines()
    # PartitionName=DEFAULT PreemptMode=OFF DefMemPerCPU=512 DefaultTime=15 MaxTime=3-00:00 TRESBillingWeights="CPU=0.5,Mem=0.3333G"
    for line in lines[::-1]:
        if line.startswith("PartitionName=DEFAULT"):
            default_mem_per_cpu = float(line.strip().split("DefMemPerCPU=")[-1].split()[0])
            return default_mem_per_cpu


def read_slurm_conf(slurm_conf, partition_name):
    with open(slurm_conf) as f:
        lines = f.readlines()
    for line in lines[::-1]:  # PartitionName is at the end of file
        if line.startswith("PartitionName={}".format(partition_name)):
            #PartitionName=prepost    Nodes=wbl[001-005,008-011] TRESBillingWeights="CPU=1.0,Mem=0.1429G"  QOS=p_prepost2 MaxTime=03:00:00 PriorityTier=1
            try:
                return get_billing_weights(line)
            except (IndexError, ValueError):
                # TRESBillingWeights not found, use default cpu and mem
                for l in lines[::-1]:
                    if l.startswith("PartitionName=DEFAULT"):
                        return get_billing_weights(l)


def calculate_requested_chours(cpu_billing_weights, mem_billing_weights, mem_per_cpu, ntasks=DEFAULT_NTASKS, cpus_per_task=DEFAULT_NTASKS, priority=False):
    # https://slurm.schedmd.com/tres.html
    # see above link for calculating formula
    total_cpus = ntasks * cpus_per_task
    total_mem = total_cpus * mem_per_cpu

    if not priority:
        requested_hours = (total_cpus * cpu_billing_weights) + (total_mem * mem_billing_weights)
    else:
        requested_hours = max(total_cpus * cpu_billing_weights, total_mem * mem_per_cpu)
    print(requested_hours)
    return requested_hours


def get_available_chours(json_file, account, username):
    with open(json_file, 'r') as f:
        json_array = json.load(f)
    print(json_array)
    for d in json_array:
        if d.get(account) is not None:
            print("adsf")
            for user_dict in d[account]:
                if user_dict.get(username) is not None:
                    print(user_dict)
                    return user_dict[username]['allocation'] - user_dict[username]['used']
                else:
                    sys.exit("No core hours usage info for {} {} from json file {}".format(account, username, json_file))
        else:
            sys.exit("No core hours usage info for {} from json file {}".format(account, json_file))


def compare_hours(requested_hours, available_hours):
    if requested_hours <= available_hours:
        subprocess.call(TEST_CMD, shell=True)
    else:
        sys.exit("Not enought core hours left, please contact Jonney")


def process_slurm_header(sl_file):
    with open(sl_file) as f:
        lines = f.readlines()
    # SBATCH --account=nesi00213
    header_dict = {h.value: None for h in SlurmHeader}

    for header in header_dict.keys():
        for line in lines:
            if "SBATCH" in line and header in line:
                value = line.strip().split("--{}=".format(header))[-1]
                header_dict[header] = value
                break
    return header_dict


def process_mem_per_cpu(mem_per_cpu):
    if isinstance(mem_per_cpu, str):
        if mem_per_cpu[-1].upper() == 'G':  # 0.5G to 0.5
            mem_per_cpu = float(mem_per_cpu[:-1])  # 521MB to 0.5
        elif mem_per_cpu[-2:] == 'MB':
            mem_per_cpu = float(mem_per_cpu[:-2]) / MB_PER_GB
    elif isinstance(mem_per_cpu, float) or isinstance(mem_per_cpu, int):
        mem_per_cpu = mem_per_cpu / MB_PER_GB  # 521MB to 0.5
    else:
        sys.exit("undefined mem per cpu")
    return mem_per_cpu


def process_header_values(slurm_header_dict, slurm_conf, hpc):
    if not slurm_header_dict[SlurmHeader.NTASKS.value]:  # None, set to default
        slurm_header_dict[SlurmHeader.NTASKS.value] = DEFAULT_NTASKS
    else: # not None, convert str to int
        slurm_header_dict[SlurmHeader.NTASKS.value] = int(slurm_header_dict[SlurmHeader.NTASKS.value])

    if not slurm_header_dict[SlurmHeader.CPUS_PER_TASK.value]:
        slurm_header_dict[SlurmHeader.CPUS_PER_TASK.value] = DEFAULT_CPUS_PER_TASK
    else:
        slurm_header_dict[SlurmHeader.CPUS_PER_TASK.value] = int(slurm_header_dict[SlurmHeader.CPUS_PER_TASK.value])

    if hpc == "mahuika":
        if not slurm_header_dict[SlurmHeader.MEM_PER_CPU.value]:
            slurm_header_dict[SlurmHeader.MEM_PER_CPU.value] = get_default_mem_per_cpu(slurm_conf)
        else:
            slurm_header_dict[SlurmHeader.MEM_PER_CPU.value] = process_mem_per_cpu(slurm_header_dict[SlurmHeader.MEM_PER_CPU.value])

    return slurm_header_dict


def get_hpc_conf(partition):
    if partition in MAUI_PARTITIONS:
        slurm_conf = '/etc/opt/slurm.conf'
        hpc = "maui"
    if partition in MAHUIKA_PARTITIONS:
        slurm_conf = '/scale_wlg_persistent/filesets/home/slurm/mahuika/etc/opt/slurm/slurm.conf'
        hpc = "mahuika"
    return hpc, slurm_conf


sf = '/home/melody/slurm.conf'
jf = '/home/melody/core_hours.json'
sl ='/home/melody/sim_bb.sl'
avai_hours = get_available_chours(jf, 'nesi00213', 'melody.zhu')
cpu_weights, mem_weights = read_slurm_conf(sf, 'large')
header_dict = process_slurm_header(sl)
header_dict = process_header_values(header_dict, "mahuika", sf)
print(header_dict)
req_hours = calculate_requested_chours(cpu_weights, mem_weights, header_dict[SlurmHeader.MEM_PER_CPU.value], header_dict[SlurmHeader.NTASKS.value], header_dict[SlurmHeader.CPUS_PER_TASK.value])
compare_hours(req_hours, avai_hours)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("slurm_to_sbatch", help="path to slurm file to be sbatched")
    parser.add_argument("--core_hour_json", default = '/home/melody/core_hours.json', help="path to core hour json file")
    parser.add_argument("--user", default=getpass.getuser(), help="path to core hour json file")
    args = parser.parse_args()

    header_dict = process_slurm_header(args.slurm_to_sbatch)

    hpc, slurm_conf = get_hpc_conf(header_dict[SlurmHeader.PARTITION.value])

    header_dict = process_header_values(header_dict, hpc, slurm_conf)

    avai_hours = get_available_chours(args.core_hour_json, header_dict[SlurmHeader.ACCOUNT.value], args.user)

    if hpc =="mahuika":
        cpu_weights, mem_weights = read_slurm_conf(slurm_conf, header_dict[SlurmHeader.PARTITION.value])
        req_hours = calculate_requested_chours(cpu_weights, mem_weights, header_dict[SlurmHeader.MEM_PER_CPU.value],
                                           header_dict[SlurmHeader.NTASKS.value],
                                           header_dict[SlurmHeader.CPUS_PER_TASK.value])
    elif hpc =="maui":
        req_hours = calculate_requested_chours(1, 1, 0,
                                               header_dict[SlurmHeader.NTASKS.value],
                                               header_dict[SlurmHeader.CPUS_PER_TASK.value])
    compare_hours(req_hours, avai_hours)