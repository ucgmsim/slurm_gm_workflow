#!/usr/bin/env python3

import sys
import os
import json
import subprocess
import argparse
import getpass
from enum import Enum

MB_PER_GB = 1024.0

DEFAULTS = {'ntasks': 1, 'nodes': 1, 'cpus-per-task': 1, 'ntasks-per-core': 1, 'cpu_weights': 1, "mem_weights": 0}

MAHUIKA_SLURM_CONF = (
    "/scale_wlg_persistent/filesets/home/slurm/mahuika/etc/opt/slurm/slurm.conf"
)
MAUI_SLUM_CONF = "/scale_wlg_persistent/filesets/home/slurm/maui/etc/opt/slurm/slurm.conf.factory"

SBATCH_BIN = "/opt/slurm/18.08.7/bin/sbatch"


# can use from qcore
class HPC(Enum):
    maui = "maui"
    mahuika = "mahuika"


# can be moved to qcore const
class SlurmHeader(Enum):
    account = "account"
    partition = "partition"
    ntasks = "ntasks"
    ntasks_per_core = "ntasks-per-core"
    ncpus = "ncpus"
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
    """Get cpu_weights and memory_weights from the line containing TresBilling attribute on mahuika"""
    # ["'CPU=1.0,Mem=0.1429G'", 'QOS=p_prepost2', 'MaxTime=03:00:00', 'PriorityTier=1']
    cpu, mem = line.strip().split("TRESBillingWeights=")[-1].split()[0].split(",")
    cpu = float(cpu.split("=")[-1])
    mem = float(mem.split("=")[-1][:-2])

    return cpu, mem


def get_default_mem_per_cpu(slurm_conf):
    """"Read a slurm.conf file and gets the default memory per cpu"""
    with open(slurm_conf, "r") as f:
        lines = f.readlines()
    # PartitionName=DEFAULT PreemptMode=OFF DefMemPerCPU=512 DefaultTime=15 MaxTime=3-00:00 TRESBillingWeights="CPU=0.5,Mem=0.3333G"
    for line in lines[::-1]:
        if line.startswith("PartitionName=DEFAULT"):
            # 512 /1024
            try:
                default_mem_per_cpu = (
                    float(line.strip().split("DefMemPerCPU=")[-1].split()[0]) / MB_PER_GB
                )
            except (IndexError, ValueError):  # maui slum.conf may not have DefMemPerCPU attribute
                default_mem_per_cpu = 0
            return default_mem_per_cpu


def read_slurm_conf(slurm_conf, partition_name):
    """
       Read a slurm.conf and returns the correspoding cpu_weights and mem_weights according to the partition_name
       Currently only used for mahuika slurm.conf
    """
    with open(slurm_conf, "r") as f:
        lines = f.readlines()
    # PartitionName is at the end of file
    for line in lines[::-1]:
        if line.startswith("PartitionName={}".format(partition_name)):
            # PartitionName=prepost    Nodes=wbl[001-005,008-011] TRESBillingWeights="CPU=1.0,Mem=0.1429G"  QOS=p_prepost2 MaxTime=03:00:00 PriorityTier=1
            try:
                return get_billing_weights(line)
            except (IndexError, ValueError):
                # TRESBillingWeights not found, use default cpu_weights and mem_weights
                for l in lines[::-1]:
                    if l.startswith("PartitionName=DEFAULT"):
                        try:
                            return get_billing_weights(l)
                        except (IndexError, ValueError):
                            return DEFAULTS['cpu_weights'], DEFAULTS["mem_weights"]


def calculate_requested_chours(
        cpu_billing_weights,
        mem_billing_weights,
        mem_per_cpu,
        ntasks,
        ntasks_per_core,
        cpus_per_task,
):
    """Calculate requested core hours for a job on mahuika"""
    # https://slurm.schedmd.com/tres.html
    # see above link for calculating formula
    total_cpus = ntasks * ntasks_per_core * cpus_per_task
    total_mem = total_cpus * mem_per_cpu

    requested_hours = (total_cpus * cpu_billing_weights) + (
        total_mem * mem_billing_weights
    )

    return requested_hours


def get_available_chours(json_file, account, username):
    """
       Read the core hours json file that's populated from the dashboard.db and
       return the available core hours for a specified user in specified account
    """
    with open(json_file, "r") as f:
        json_array = json.load(f)
    for d in json_array:
        if d.get(account) is not None:
            for user_dict in d[account]:
                if user_dict.get(username) is not None:
                    return (
                        user_dict[username]["allocation"] - user_dict[username]["used"]
                    )
                else:
                    sys.exit(
                        "No core hours usage info for {} {} from json file {}".format(
                            account, username, json_file
                        )
                    )
        else:
            sys.exit(
                "No core hours usage info for {} from json file {}".format(
                    account, json_file
                )
            )


def process_slurm_header(sl_file):
    """Read a slurm file and returns the wanted header values as a dictionary"""
    with open(sl_file) as f:
        lines = f.readlines()
    # SBATCH --account=nesi00213
    header_dict = {h.value: None for h in SlurmHeader}
    for line in lines:
        if line.startswith("#") and "SBATCH" in line.upper():
            for header in header_dict.keys():
                if "{}=".format(header) in line:
                    value = line.strip().split("--{}=".format(header))[-1]
                    header_dict[header] = value
                    break
    return header_dict


def process_mem_per_cpu(mem_per_cpu):
    """Convert a mem_per_cpu string to float"""
    if isinstance(mem_per_cpu, str):
        if mem_per_cpu[-1].upper() == "G" or mem_per_cpu[-2:].upper() == "GB":
            # 0.5G to 0.5
            mem_per_cpu = float(mem_per_cpu[:-1])
    else:  # no unit or unit=MB, All default to MB
        mem_per_cpu = float(mem_per_cpu[:-2]) / MB_PER_GB
    return mem_per_cpu


def process_header_values(slurm_header_dict, slurm_conf):
    """
       Process header dict values
       For maui, mem_per_cpu is None as mem is not calculated against billing
    """
    for header in [SlurmHeader.ntasks, SlurmHeader.cpus_per_task, SlurmHeader.ntasks_per_core]:
        if not slurm_header_dict[header.value]:
            slurm_header_dict[header.value] = DEFAULTS[header.value]
        else:
            slurm_header_dict[header.value] = int(slurm_header_dict[header.value])

    if not slurm_header_dict[SlurmHeader.mem_per_cpu.value]:
        slurm_header_dict[SlurmHeader.mem_per_cpu.value] = get_default_mem_per_cpu(
            slurm_conf
        )
    else:
        slurm_header_dict[SlurmHeader.mem_per_cpu.value] = process_mem_per_cpu(
            slurm_header_dict[SlurmHeader.mem_per_cpu.value]
        )

    return slurm_header_dict


def get_hpc_conf(partition):
    """Get hpc name and path to slurm.conf according to partition name in the slurm file header"""
    if partition in [p.value for p in MauiPartition]:
        slurm_conf = MAUI_SLUM_CONF  # dummy, not used
        hpc = HPC.maui.value
    elif partition in [p.value for p in MahuikaPartition]:
        slurm_conf = MAHUIKA_SLURM_CONF
        hpc = HPC.mahuika.value
    else:
        sys.exit("{} partition does not exist")
    return hpc, slurm_conf


def compare_hours_and_sbatch(requested_hours, available_hours, hpc, sl_to_sbatch, sbatch_extra_args):
    """
       Compare requested hour with available hours
       and either submit the job or reject
    """
    if requested_hours <= available_hours:
        cmd = "{} -M {} {} {}".format(SBATCH_BIN, hpc, sl_to_sbatch, sbatch_extra_args)
        subprocess.call(cmd, shell=True)
    else:
        sys.exit("Not enough core hours left, please apply for more")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("slurm_to_sbatch", help="path to slurm file to be sbatched")
    parser.add_argument('sbatch_extra_args', nargs='*', help="all args following xx.sl")
    parser.add_argument(
        "--core_hour_json",
        default="core_hours.json",
        help="path to core hour json file getting from dashboard.db, default is core_hours.json",
    )
    parser.add_argument(
        "--user", default=getpass.getuser(), help="the user who's sbatching the job"
    )
    args = parser.parse_args()

    assert os.path.exists(args.slurm_to_sbatch)
    assert os.path.exists(args.core_hour_json)

    # Get raw headers
    header_dict = process_slurm_header(args.slurm_to_sbatch)
    print(header_dict)
    # Get hpc_name and path to slurm.conf
    hpc, slurm_conf = get_hpc_conf(header_dict[SlurmHeader.partition.value])

    # Process headers for calculation
    header_dict = process_header_values(header_dict, slurm_conf)
    print(header_dict)
    # Get available hours
    avai_hours = get_available_chours(
        args.core_hour_json, header_dict[SlurmHeader.account.value], args.user
    )

    # Calculate requested hours
    cpu_weights, mem_weights = read_slurm_conf(
        slurm_conf, header_dict[SlurmHeader.partition.value]
    )
    print("cpu weights {}\nmem_weights {}".format(cpu_weights, mem_weights))
    req_hours = calculate_requested_chours(
        cpu_weights,
        mem_weights,
        header_dict[SlurmHeader.mem_per_cpu.value],
        header_dict[SlurmHeader.ntasks.value],
        header_dict[SlurmHeader.ntasks_per_core.value],
        header_dict[SlurmHeader.cpus_per_task.value],
    )

    print("Reqested hours {}".format(req_hours))
    print("Available_hours {}".format(avai_hours))

    # Decide whether to submit the job or not
    compare_hours_and_sbatch(req_hours, avai_hours, hpc, args.slurm_to_sbatch, args.sbatch_extra_args)


if __name__ == "__main__":
    main()

