#!/usr/bin/env python3

import sys
import json
import subprocess

TEST_CMD = 'mkdir test_sbatch'


def get_billing_weights(line):
    # ["'CPU=1.0,Mem=0.1429G'", 'QOS=p_prepost2', 'MaxTime=03:00:00', 'PriorityTier=1']
    cpu, mem = line.strip().split("TRESBillingWeights=")[-1].split()[0].split(',')
    cpu = float(cpu.split('=')[-1])
    mem = float(mem.split('=')[-1][:-2])
    print(cpu,mem)
    return cpu, mem


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


def calculate_requested_chours(ntasks, cpus_per_task, mem_per_cpu, cpu_billing_weights, mem_billing_weights, priority=False):
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


def compare_hours(requested_hours, available_hours):
    if requested_hours <= available_hours:
        subprocess.call(TEST_CMD, shell=True)
    else:
        sys.exit("Not enought core hours left, please contact Jonney")


sf = '/home/melody/slurm.conf'
jf = '/home/melody/core_hours.json'
avai_hours = get_available_chours(jf, 'nesi00213', 'melody.zhu')
cpu_weights, mem_weights = read_slurm_conf(sf, 'large')
req_hours = calculate_requested_chours(2, 8, 16, cpu_weights, mem_weights)
compare_hours(req_hours, avai_hours)