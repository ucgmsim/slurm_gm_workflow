#TODO: 1.ADD ERROR EXCEPTION 2.ADD COMMENTS
# There are 4 possible commands:
# python write_jsons.py /nesi/nobackup/nesi00213/RunFolder/Cybershake/v18p6_batched/v18p6_exclude_1k_batch_2/Runs/
# python write_jsons.py /nesi/nobackup/nesi00213/RunFolder/Cybershake/v18p6_batched/v18p6_exclude_1k_batch_2/Runs/ -sj
# python write_jsons.py /nesi/nobackup/nesi00213/RunFolder/Cybershake/v18p6_batched/v18p6_exclude_1k_batch_2/Runs/HopeCW -sr
# python write_jsons.py /nesi/nobackup/nesi00213/RunFolder/Cybershake/v18p6_batched/v18p6_exclude_1k_batch_2/Runs/HopeCW -sr -sj

import os
import sys
import json
import argparse
import subprocess

from qcore.utils import setup_dir

# the following keys should be consistent(in the same order) with the output params specified in proc_mpi_sl.template under each fault dir
# TODO auto retrive from the templates rather than hardcode
BB_KEYS = ['cores', 'run_time', 'fd_count', 'dt']
HF_KEYS = ['cores', 'run_time', 'fd_count', 'nt', 'nsub_stoch']
LF_KEYS = ['cores', 'run_time', 'nt', 'nx', 'ny', 'nz']
KEY_DICT = {'BB': BB_KEYS, 'HF': HF_KEYS, 'LF': LF_KEYS}

CH_LOG = 'ch_log'
JSON_DIR = 'jsons'
ALL_META_JSON = 'all_sims.json'
PARAMS_BASE = 'params_base.py'


def get_all_sims_dict(fault_dir):
    """
    :param fault_dir: abs path to a single fault dir
    :return: all_sims_dict or None
    """
    ch_log_dir = os.path.join(fault_dir, CH_LOG)
    if os.path.isdir(ch_log_dir):
        all_sims_dict = {}
        hh = get_hh(fault_dir)
        for f in os.listdir(ch_log_dir):  # iterate through all realizations
            f_suffix = f[:2]
            with open(os.path.join(ch_log_dir, f), 'r') as log_file:
                buf = log_file.readlines()
            raw_data = buf[0].strip().split()
            realization = raw_data[0]
            data = raw_data[2:]
            if realization in all_sims_dict.keys():
                all_sims_dict[realization].update({f_suffix: {}})
            else:
                all_sims_dict[realization] = {'common': {'hh': hh}, f_suffix: {}}
            for i in range(len(data)):
                key = KEY_DICT[f_suffix][i]
                all_sims_dict[realization][f_suffix][key] = data[i]
            if f_suffix == 'HF' and all_sims_dict[realization][f_suffix].get('total_memo_usage') == None:
                realization_rlog_dir = os.path.join(fault_dir, 'LF', realization, 'Rlog')
                all_sims_dict[realization][f_suffix]['total_memo_usage'] = get_one_realization_memo_cores(realization_rlog_dir)
        return all_sims_dict
    else:
        print("{} does not have a ch_log dir\n If your input run_folder path points to a single fault dir then you need to add '-sf' option ".format(fault_dir))


def get_one_realization_memo_cores(realization_rlog_dir):
    """
    :param realization_rlog_dir: /nesi/nobackup/nesi00213/RunFolder/Cybershake/v18p6_batched/v18p6_exclude_1k_batch_2/Runs/HopeConwayOS/LF/HopeConwayOS_HYP01-36_S1244/Rlog
    :return:total_memory usage for each realization in GB
    """
    os.chdir(realization_rlog_dir)
    cmd = "grep 'total for model' *.rlog"
    output = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True).communicate()[0]
    total = 0
    unit = ''

    for line in output.strip().split('\n'):
        memo, unit = line.strip().split("=")[1].strip().split()
        total += float(memo)
    if unit == 'Mb':
        total /= 1024.
        unit = 'GB'
    return "{} {}".format(total, unit)


def get_hh(fault_dir):
    """
    :param fault_dir:
    :return: hh value string
    """
    os.chdir(fault_dir)
    params_base = PARAMS_BASE
    if os.path.isfile(params_base):
        cmd = "grep 'hh =' params_base.py"
        output = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True).communicate()[0]  # output = "hh = '0.4' #must be in formate like 0.200 (3decimal places)\n"
        hh = output.split("#")[0].split("=")[1].strip().replace("'", '')  # "'0.4'" ---> '0.4'
        return hh


def write_json(data_dict, out_dir, out_name):
    """
    writes a json file
    :param data_dict: sim_dict
    :param out_dir: fault_dir
    :param out_name: output json file name
    :return:
    """
    json_data = json.dumps(data_dict)
    abs_outpath = os.path.join(out_dir, out_name)

    try:
        with open(abs_outpath, 'w') as out_file:
            out_file.write(json_data)
    except Exception as e:
        sys.exit(e)


def write_fault_jsons(fault_dir, single_json):
    """
    write json file(s) for a single fault
    :param fault_dir:
    :param single_json: boolean, user input
    :return:
    """
    all_sims_dict = get_all_sims_dict(fault_dir)
    if all_sims_dict != None:
        out_dir = os.path.join(fault_dir, JSON_DIR)
        setup_dir(out_dir)

        if single_json:
            write_json(all_sims_dict, out_dir, ALL_META_JSON)
        else:
            for realization, realization_dict in all_sims_dict.items():
                out_name = "{}.json".format(realization)
                write_json(realization_dict, out_dir, out_name)
        print("Json file(s) write to {}".format(out_dir))


def write_faults_jsons(run_folder, single_fault, single_json):
    """
    write json file(s) for a single or all faults
    :param run_folder: user input
    :param single_fault: boolean, user input
    :param single_json: boolean, user input
    :return:
    """
    if single_fault:
        write_fault_jsons(run_folder, single_json)
    else:
        for fault in os.listdir(run_folder):
            fault_dir = os.path.join(run_folder, fault)
            if os.path.isdir(fault_dir):
                write_fault_jsons(fault_dir, single_json)


def validate_run_folder(parser, arg_run_folder):
    """
    validates user input path
    :param parser:
    :param arg_run_folder: user input path
    :return:
    """
    if not os.path.isdir(arg_run_folder):
        parser.error("Folder {} does not exist".format(arg_run_folder))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('run_folder',
                        help="path to cybershake run_folder eg'/nesi/nobackup/nesi00213/RunFolder/Cybershake/v18p6_batched/v18p6_exclude_1k_batch_2/Runs/' or '/nesi/nobackup/nesi00213/RunFolder/Cybershake/v18p6_batched/v18p6_exclude_1k_batch_2/Runs/Hollyford' ")
    parser.add_argument('-sj', '--single_json', action='store_true',
                        help="Please add '-sj' to indicate that you only want to output one single_json json file that contains all realizations. Default output one json file for each realization")
    parser.add_argument('-sf', '--single_fault', action='store_true',
                        help="Please add '-sf' to indicate that run_folder path points to a single fault eg, add '-sf' if run_folder is '/nesi/nobackup/nesi00213/RunFolder/Cybershake/v18p6_batched/v18p6_exclude_1k_batch_2/Runs/Hollyford'")

    args = parser.parse_args()
    validate_run_folder(parser, args.run_folder)
    write_faults_jsons(args.run_folder, args.single_fault, args.single_json)


if __name__ == '__main__':
    main()

