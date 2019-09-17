#!/usr/bin/env python3

import sys
import os
import json
import subprocess
import argparse
import getpass
from enum import Enum

TEST_CMD = 'mkdir test_sbatch'
MB_PER_GB = 1024.

DEFAULT_NTASKS = 1
DEFAULT_CPUS_PER_TASK = 1

MAHUIKA_SLURM_CONF = '/scale_wlg_persistent/filesets/home/slurm/mahuika/etc/opt/slurm/slurm.conf'
MAUI_SLUM_CONF = '/etc/opt/slurm.conf'


# can use from qcore
class HPC(Enum):
    maui = "maui"
    mahuika = "mahuika"


# can be moved to qcore const
class SlurmHeader(Enum):
    account = "account"
    partition = "partition"
    ntasks = "ntasks"
    cpus_per_task = "cpus-per-task"
    mem_per_cpu = "mem-per-cpu"


class MahuikaPartition(Enum):
    large = "large"
    long = "long"
    perpost = "prepost"
    bigmem = "bigmem"
    hugmem = "hugemem"
    ga_bigmem = "ga_bigmem"
    ga_hugemem = "ga_hugemem"
    gpu = "gpu"
    igpu = "igpu"


class MauiPartition(Enum):
    nesi_research = "nesi_research"


def get_billing_weights(line):
    # ["'CPU=1.0,Mem=0.1429G'", 'QOS=p_prepost2', 'MaxTime=03:00:00', 'PriorityTier=1']
    cpu, mem = line.strip().split("TRESBillingWeights=")[-1].split()[0].split(',')
    cpu = float(cpu.split('=')[-1])
    mem = float(mem.split('=')[-1][:-2])
    print(cpu,mem)
    return cpu, mem


def get_default_mem_per_cpu(slurm_conf):
    with open(slurm_conf, 'r') as f:
        lines = f.readlines()
    # partitionName=DEFAULT PreemptMode=OFF DefMemPerCPU=512 DefaultTime=15 MaxTime=3-00:00 TRESBillingWeights="CPU=0.5,Mem=0.3333G"
    for line in lines[::-1]:
        if line.startswith("PartitionName=DEFAULT"):
            default_mem_per_cpu = float(line.strip().split("DefMemPerCPU=")[-1].split()[0]) / MB_PER_GB
            return default_mem_per_cpu


def read_slurm_conf(slurm_conf, partition_name):
    with open(slurm_conf, 'r') as f:
        lines = f.readlines()
    for line in lines[::-1]:  # partitionName is at the end of file
        if line.startswith("PartitionName={}".format(partition_name)):
            #partitionName=prepost    Nodes=wbl[001-005,008-011] TRESBillingWeights="CPU=1.0,Mem=0.1429G"  QOS=p_prepost2 MaxTime=03:00:00 PriorityTier=1
            try:
                return get_billing_weights(line)
            except (IndexError, ValueError):
                # TRESBillingWeights not found, use default cpu and mem
                for l in lines[::-1]:
                    if l.startswith("PartitionName=DEFAULT"):
                        return get_billing_weights(l)


def calculate_requested_chours(cpu_billing_weights, mem_billing_weights, mem_per_cpu, ntasks=DEFAULT_NTASKS, cpus_per_task=DEFAULT_CPUS_PER_TASK, priority=False):
    # https://slurm.schedmd.com/tres.html
    # see above link for calculating formula
    total_cpus = ntasks * cpus_per_task
    total_mem = total_cpus * mem_per_cpu
    print(cpu_billing_weights, mem_billing_weights, mem_per_cpu, ntasks, cpus_per_task)
    if not priority:
        requested_hours = (total_cpus * cpu_billing_weights) + (total_mem * mem_billing_weights)
    else:
        requested_hours = max(total_cpus * cpu_billing_weights, total_mem * mem_per_cpu)
    print(requested_hours)
    return requested_hours


def get_available_chours(json_file, account, username):
    with open(json_file, 'r') as f:
        json_array = json.load(f)
    for d in json_array:
        if d.get(account) is not None:
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
    elif (isinstance(mem_per_cpu, float) or isinstance(mem_per_cpu, int)) and mem_per_cpu > 100:
        mem_per_cpu = mem_per_cpu / MB_PER_GB  # 521MB to 0.5
    else:
        sys.exit("undefined mem per cpu")
    return mem_per_cpu


def process_header_values(slurm_header_dict, slurm_conf, hpc):
    if not slurm_header_dict[SlurmHeader.ntasks.value]:  # None, set to default
        slurm_header_dict[SlurmHeader.ntasks.value] = DEFAULT_NTASKS
    else: # not None, convert str to int
        slurm_header_dict[SlurmHeader.ntasks.value] = int(slurm_header_dict[SlurmHeader.ntasks.value])

    if not slurm_header_dict[SlurmHeader.cpus_per_task.value]:
        slurm_header_dict[SlurmHeader.cpus_per_task.value] = DEFAULT_CPUS_PER_TASK
    else:
        slurm_header_dict[SlurmHeader.cpus_per_task.value] = int(slurm_header_dict[SlurmHeader.cpus_per_task.value])

    if hpc == HPC.mahuika.value:
        if not slurm_header_dict[SlurmHeader.mem_per_cpu.value]:
            slurm_header_dict[SlurmHeader.mem_per_cpu.value] = get_default_mem_per_cpu(slurm_conf)
        else:
            slurm_header_dict[SlurmHeader.mem_per_cpu.value] = process_mem_per_cpu(slurm_header_dict[SlurmHeader.mem_per_cpu.value])

    return slurm_header_dict


def get_hpc_conf(partition):
    if partition in [p.value for p in MauiPartition]:
        slurm_conf = MAUI_SLUM_CONF
        hpc = HPC.maui.value
    elif partition in [p.value for p in MahuikaPartition]:
        slurm_conf = MAHUIKA_SLURM_CONF
        hpc = HPC.mahuika.value
    else:
        sys.exit("{} partition does not exit")
    print("hpc {}, partition {}".format(hpc, partition))
    return hpc, slurm_conf


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("slurm_to_sbatch", help="path to slurm file to be sbatched")
    parser.add_argument("--core_hour_json", default='core_hours.json', help="path to core hour json file, default is core_hours.json")
    parser.add_argument("--user", default=getpass.getuser(), help="the user who's sbatching the job")
    args = parser.parse_args()
    assert os.path.exists(args.slurm_to_sbatch)
    assert os.path.exists(args.core_hour_json)
    header_dict = process_slurm_header(args.slurm_to_sbatch)

    hpc, slurm_conf = get_hpc_conf(header_dict[SlurmHeader.partition.value])

    header_dict = process_header_values(header_dict, slurm_conf, hpc)
    print("processed header_dict", header_dict)
    avai_hours = get_available_chours(args.core_hour_json, header_dict[SlurmHeader.account.value], args.user)

    if hpc == HPC.mahuika.value:
        cpu_weights, mem_weights = read_slurm_conf(slurm_conf, header_dict[SlurmHeader.partition.value])
        req_hours = calculate_requested_chours(cpu_weights, mem_weights, header_dict[SlurmHeader.mem_per_cpu.value],
                                           header_dict[SlurmHeader.ntasks.value],
                                           header_dict[SlurmHeader.cpus_per_task.value])
    elif hpc == HPC.maui.value:
        # maui only bill against total cpus, no cpu_weights or mem_weights
        req_hours = header_dict[SlurmHeader.ntasks.value] * header_dict[SlurmHeader.cpus_per_task.value]
    print("reqested hours {}".format(req_hours))
    print("available_hours {}".format(avai_hours))
    compare_hours(req_hours, avai_hours)


if __name__ == "__main__":
    main()
