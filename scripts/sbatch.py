#!/usr/bin/env python3

import argparse
from enum import Enum
import getpass
import datetime
import json
from math import ceil
import os
import re
import subprocess
import sys

MB_PER_GB = 1024.0

MAHUIKA_SLURM_CONF = "/scale_wlg_persistent/filesets/home/slurm/mahuika/etc/opt/slurm/slurm.conf"
MAUI_SLUM_CONF = "/scale_wlg_persistent/filesets/home/slurm/maui/etc/opt/slurm/slurm.conf"
MAUI_ANCIL_SLUM_CONF = "/scale_wlg_persistent/filesets/home/slurm/maui_ancil/etc/opt/slurm/slurm.conf"

SBATCH_BIN = "/opt/slurm/18.08.7/bin/sbatch"


#TODO: classes can be included into qcore libs ( if qcore become a required dependency)
class HPC(Enum):
    #since default value for multithread is True, the numbers are logical cores
    maui = (0,"maui")
    maui_ancil = (1,"maui_ancil")
    mahuika = (2,"mahuika",)

    def __new__(cls, value, id):
        obj = object.__new__(cls)
        obj._value_ = value
        obj.id = id
        return obj


#Trace-able resources and the default weight value for Billing
class TRESWeight(Enum):
    cpu = (0,"CPU", 1.0)
    mem = (1,"Mem", 0.0)
    gpu = (2,"gpu", 0.0)

    def __new__(cls, value, id, weight):
        obj = object.__new__(cls)
        obj._value_ = value
        obj.id = id
        obj.default_weight = weight
        return obj


# levels of parameters in config
class varLevel(Enum):
    specific = 0,'specific'
    default = 1,'DEFAULT'
    root = 2,'root'
    def __new__(cls, value, id):
        obj = object.__new__(cls)
        obj._value_ = value
        obj.id = id
        return obj 


# can be moved to qcore const
class SlurmHeader(Enum):
    account = (0,"account", str, 'nesi00213')
    partition = (1,"partition", str, None)
    ntasks = (2,"ntasks", int, 1)
    ntasks_per_core = (3,"ntasks-per-core", int, 1)
    cpus_per_task = (4,"cpus-per-task", int, 1)
    mem_per_cpu = (5,"mem-per-cpu", str, None)
    ntasks_per_node = (6, "ntasks-per-node", int, 0)
    time = (7, "time", int, 0) #in seconds
    #flags
    nomultithread = (8,"hint=nomultithread", bool, False)
    exclusive = (9,"exclusive", bool, False)

    def __new__(cls, value, id,type, default):
        obj = object.__new__(cls)
        obj._value_ = value
        obj.id = id
        obj.type = type
        obj.default = default
        return obj


#TODO:nest partition classes with HPC classes so no double define
class MahuikaPartition(Enum):
    large = (0, "large", 72, 108*MB_PER_GB)
    long = (1, "long", 72, 108*MB_PER_GB)
    perpost = (2, "prepost", 72, 480*MB_PER_GB)
    bigmem = (3, "bigmem", 72, 480*MB_PER_GB)
    hugmem = (4, "hugemem", 128, 4000*MB_PER_GB)
    ga_bigmem = (5, "ga_bigmem", 72, 480*MB_PER_GB)
    ga_hugemem = (6, "ga_hugemem", 128, 4000*MB_PER_GB)
    gpu = (7, "gpu", 8, 108*MB_PER_GB)
    igpu = (8, "igpu", 8, 108*MB_PER_GB)

    def __new__(cls, value, id, cpu_per_node, mem_per_node):
        obj = object.__new__(cls)
        obj._value_ = value
        obj.id = id
        obj.cpus = cpu_per_node
        obj.mem = mem_per_node
        obj.config_path = MAHUIKA_SLURM_CONF
        obj.hpc = HPC.mahuika.value
        return obj


class MauiPartition(Enum):
    nesi_research = (0, "nesi_research", 80, 160*MB_PER_GB)

    def __new__(cls, value, id, cpu_per_node, mem_per_node):
        obj = object.__new__(cls)
        obj._value_ = value
        obj.id = id
        obj.cpus = cpu_per_node
        obj.mem = mem_per_node
        obj.config_path = MAUI_SLUM_CONF
        obj.hpc = HPC.maui.value
        return obj


class MauiAncilPartition(Enum):
    nesi_prepost = (0, "nesi_prepost", 80, 720*MB_PER_GB)
    nesi_gpu = (1,"nesi_gpu", 4, 12*MB_PER_GB)
    nesi_igpu = (2,"nesi_igpu", 4, 12*MB_PER_GB)
    
    def __new__(cls, value, id, cpu_per_node, mem_per_node):
        obj = object.__new__(cls)
        obj._value_ = value
        obj.id = id
        obj.cpus = cpu_per_node
        obj.mem = mem_per_node
        #TODO: as stated above, nest these classes
        obj.config_path = MAUI_ANCIL_SLUM_CONF
        obj.hpc = HPC.maui_ancil.value
        return obj


def get_value_from_line(line, keyword, all2float=False):
    value = line.strip().split(keyword)[-1].split()[0]
    #removes non-digital number if = or : is included
    #must be careful that the string
    if all2float:
        value = float(re.findall(r"[-+]?\d*\.\d+|\d+", value)[0])
    
    return value


def get_value_from_conf(slurm_conf, partition,keyword, all2float=False):
    """"Read a slurm.conf file and gets the default memory per cpu"""

    with open(slurm_conf, "r") as f:
        lines = f.readlines()
    
    #example line in conf:
    # PartitionName=DEFAULT PreemptMode=OFF DefMemPerCPU=512 DefaultTime=15 MaxTime=3-00:00 TRESBillingWeights="CPU=0.5,Mem=0.3333G"

    #initializing some values
    line_with_keyword = []
    partition_keyword_value = None
    default_partition_keyword_value = None
    default_keyword_value = None
    
    for line in lines[::-1]:
        #scan through whole confige file and find lines with keyword
        if keyword in line:
            line_with_keyword.append(line)
    # raise error if none were found
    if len(line_with_keyword) == 0:
        raise ValueError("failed to find keyword:{} in config".format(keyword))
    else:
        #try find value from partion first. if not, try partition=DEFAULT, else try a global value
        for line in line_with_keyword:
            #removes potential unexpected indent
            line = line.strip()
            if line.startswith("PartitionName={}".format(partition)):
                partition_keyword_value = get_value_from_line(line, keyword, all2float) 
            elif line.startswith("PartitionName={}".format(varLevel.default.id)):
                default_partition_keyword_value =  get_value_from_line(line, keyword, all2float) 
            elif line.startswith(keyword):
                default_keyword_value = get_value_from_line(line, keyword,all2float) 
        # return a value from narrow to broad
        if partition_keyword_value is not None:
            return partition_keyword_value
        elif default_partition_keyword_value is not None:
            return default_partition_keyword_value
        elif default_keyword_value is not None:
            return default_keyword_value
        else:
            #last catch of errors if something unexpected happened
            raise ValueError("keyword:{} were found but none were parsed correctly".format(keyword))


def weight_dict_from_line(line):
    #replace ',' with ' ' if multiple TRES were defined
    line = line.replace(",", " ").strip()
    tmp_dict = []
    for res in TRES:
        tmp_dict[res.value] = get_value_from_line(line, res.id, all2float=True)
    
    return tmp_dict


def billing_weight_from_conf(slurm_conf, partition_name):
    """
       Read a slurm.conf and returns the correspoding cpu_weights and mem_weights according to the partition_name
       if nothing were found, a default of cpu=1.0 mem=0 will be used
    """
    with open(slurm_conf, "r") as f:
        lines = f.readlines()
    #initialize
    TRES_dict = {vl.value: None for vl in varLevel}
    
    # PartitionName is at the end of file
    for line in lines[::-1]:
        #scans for all lines constains TRESBillingWeights
        line_with_keyword = []
        keyword = "TRESBillingWeights"
        if keyword in line:
            line_with_keyword.append(line)
    #check if anything is greped
    if len(line_with_keyword) == 0:
        #none were found, using default values
        TRES_dict[varLevel.root.value] = {}
        for res in TRESWeight:
            TRES_dict[varLevel.root.value][res.value] = res.default_weight
    else:
        #lines with TRES found in config
        for line in line_with_keyword:
            #removes potential unexpected indent
            line = line.strip() 
            if line.startswith("PartitionName={}".format(partition_name)):
            # PartitionName=prepost    Nodes=wbl[001-005,008-011] TRESBillingWeights="CPU=1.0,Mem=0.1429G"  QOS=p_prepost2 MaxTime=03:00:00 PriorityTier=1
                TRES_dict[varLevel.specific.value] = weight_dict_from_line(get_value_from_line(line, keyword))
            elif line.startswith("PartitionName={}".format(varLevel.default.id)):
                TRES_dict[varLevel.default.value] = weight_dict_from_line(get_value_from_line(line, keyword))
            elif line.startswith(keyword):
                TRES_dict[varLevel.root.value] = weight_dict_from_line(get_value_from_line(line, keyword))
    #return the value dictionary bottom-up
    for i in range(0,len(varLevel)):
        if TRES_dict[i] is not None:
            return TRES_dict[i]


def shared_TRES_conf(slurm_conf, partition_name):
    """
    read the config and see if Shared=Exclusive is defined
    if defined, allocation are based on nodes instead of cpus
    """
    keyword="Shared="
    #NOTE: this keyword may change if NeSI updates the slurm version
    #"OverSubscribe" instead of "Shared"
    try:
        value = get_value_from_conf(slurm_conf, partition_name, keyword)
        if value.upper() == "EXCLUSIVE":
            return False
        else:
            return True        
    except ValueError:
        print("no shared definition found, assuming TRES are shared")
        return True
    

def calculate_requested_chours(
        cpu_billing_weights,
        mem_billing_weights,
        total_seconds_requested,
        mem_per_cpu,
        ntasks,
        ntasks_per_core,
        cpus_per_task,
        shared,
        cpu_per_node,
        mem_per_node,
        ntasks_per_node,
        nodes=None,
):
    """Calculate requested core hours for a job """
    # https://slurm.schedmd.com/tres.html
    # see above link for calculating formula
    if (cpus_per_task is not SlurmHeader.cpus_per_task.default):
        total_cpus = ntasks * cpus_per_task
    else:
        total_cpus = ntasks / ntasks_per_core
    if shared == False:
        nodes = ceil(total_cpus/cpu_per_node)
        if ntasks_per_node is not SlurmHeader.ntasks_per_node.default:
            nodes = nodes * (ntasks/ntasks_per_node)
        total_cpus = nodes * cpu_per_node
        total_mem  = nodes * mem_per_node
    else:
        total_mem = total_cpus * mem_per_cpu
    #TODO:GPU may need to be included
    requested_hours = (total_seconds_requested * (total_cpus * cpu_billing_weights)) + (
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
            #no data for a user is found while account exist
            sys.exit(
                "No core hours usage info for {} {} from json file {}".format(
                    account, username, json_file))
    #all project names looped raise error
    raise IndexError("No account info for {} from json file {}".format(account, json_file))


def process_slurm_header(sl_file):
    """Read a slurm file and returns the wanted header values as a dictionary"""
    with open(sl_file) as f:
        lines = f.readlines()
    #example line: SBATCH --header=value
    header_dict = {h.value: h.default for h in SlurmHeader}
    for line in lines:
        if line.upper().startswith("#SBATCH"):
            for header in SlurmHeader:
                if header.type is not bool and "--{}=".format(header.id) in line:
                    value = line.strip().split("--{}=".format(header.id))[-1]
                    #special case for WCT(time) parameter
                    if header is SlurmHeader.time:
                        #a day is specified, i.e. "%d-%H:%M:%S"
                        wct_list = list( map(int, re.findall(r"[-+]?\d*\.\d+|\d+", value)))
                        #filter the value to contain 4 fields, day, h, m, s
                        while len(wct_list) < 4: 
                            wct_list.insert(0,0)
                        value = datetime.timedelta(days=wct_list[0],hours=wct_list[1],minutes=wct_list[2],seconds=wct_list[3]).total_seconds()
                    header_dict[header.value] = header.type(value)
                    break
                elif header.type is bool and "--{}".format(header.id) in line:
                    header_dict[header.value] = True
                    
    return header_dict


def process_mem_per_cpu(mem_per_cpu):
    """Convert a mem_per_cpu string to float"""
    if isinstance(mem_per_cpu, str):
            value = float(re.findall(r"[-+]?\d*\.\d+|\d+", mem_per_cpu)[0])
    else:
        value = mem_per_cpu
    if 'G' not in mem_per_cpu.upper():  # no unit (default =MB) or unit=MB, transforming to GB
        value = float(value) / MB_PER_GB
    
    return value


def process_header_values(slurm_header_dict, slurm_conf):
    """
       Process header dict values
       get defaul values if not provided
    """
    for header in [SlurmHeader.ntasks, SlurmHeader.cpus_per_task, SlurmHeader.ntasks_per_core]:
        if not slurm_header_dict[header.value]:
            slurm_header_dict[header.value] = header.default
            slurm_header_dict[header.value] = header.default
        else:
            slurm_header_dict[header.value] = int(slurm_header_dict[header.value])
    #special treatment for WCT.
    # convert str to datetime -> datetime to second
    if not slurm_header_dict[SlurmHeader.mem_per_cpu.value]:
        slurm_header_dict[SlurmHeader.mem_per_cpu.value] = process_mem_per_cpu(get_value_from_conf(
            slurm_conf,
            slurm_header_dict.partition,
            keyword="DefMemPerCPU",
            all2float=True,
        ))
    else:
        slurm_header_dict[SlurmHeader.mem_per_cpu.value] = process_mem_per_cpu(
            slurm_header_dict[SlurmHeader.mem_per_cpu.value]
        )
    #rule for share node resource
    slurm_header_dict[SlurmHeader.exclusive.value]=shared_TRES_conf(slurm_conf, slurm_header_dict[SlurmHeader.partition.value]) 
    return slurm_header_dict


def get_hpc_conf(partition):
    """Get hpc name and path to slurm.conf according to partition name in the slurm file header"""
    for machine in [MauiPartition,MauiAncilPartition,MahuikaPartition]:
        for p in machine:
            if partition in p.id:
                slurm_conf = p.config_path  
                hpc = p.hpc
                cpu_per_node = p.cpus
                mem_per_node = p.mem
            
                return hpc, slurm_conf, cpu_per_node, mem_per_node
    raise IndexError("no matching partition name found")


def compare_hours_and_sbatch(requested_hours, available_hours, hpc, sl_to_sbatch, sbatch_extra_args):
    """
       Compare requested hour with available hours
       and either submit the job or reject
    """
    if requested_hours <= available_hours:
        cmd = "{} -M {} {} {}".format(SBATCH_BIN, hpc, sl_to_sbatch, sbatch_extra_args)
        print(cmd)
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
    # Get hpc_name and path to slurm.conf base on partition name
    hpc, slurm_conf, cpu_per_node, mem_per_node = get_hpc_conf(header_dict[SlurmHeader.partition.value])

    # Process headers for calculation
    header_dict = process_header_values(header_dict, slurm_conf)
    # Get available hours
    avai_hours = get_available_chours(
        args.core_hour_json, header_dict[SlurmHeader.account.value], args.user
    )

    # Calculate requested hours
    TRESBillingWeight_dict = billing_weight_from_conf(
        slurm_conf, header_dict[SlurmHeader.partition.value]
    )
    print(header_dict)
    print("TRES Billing Weight : {}".format(TRESBillingWeight_dict))
    req_hours = calculate_requested_chours(
        TRESBillingWeight_dict[TRESWeight.cpu.value],
        TRESBillingWeight_dict[TRESWeight.mem.value],
        header_dict[SlurmHeader.time.value],
        header_dict[SlurmHeader.mem_per_cpu.value],
        header_dict[SlurmHeader.ntasks.value],
        header_dict[SlurmHeader.ntasks_per_core.value],
        header_dict[SlurmHeader.cpus_per_task.value],
        header_dict[SlurmHeader.exclusive.value],
        cpu_per_node,
        mem_per_node,
        header_dict[SlurmHeader.ntasks_per_node.value],
    )

    print("Reqested hours {}".format(req_hours))
    print("Available_hours {}".format(avai_hours))

    # Decide whether to submit the job or not
    compare_hours_and_sbatch(req_hours, avai_hours, hpc, args.slurm_to_sbatch, args.sbatch_extra_args)


if __name__ == "__main__":
    main()

