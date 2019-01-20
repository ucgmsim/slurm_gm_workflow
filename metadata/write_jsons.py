#!/usr/bin/env python3
""""
Creates a .json metadata file for each fault in the specified Runs folder

# Examples with the different flags
# python3 write_jsons.py /nesi/nobackup/nesi00213/RunFolder/Cybershake/v18p6_batched/v18p6_exclude_1k_batch_2/Runs/
# python3 write_jsons.py /nesi/nobackup/nesi00213/RunFolder/Cybershake/v18p6_batched/v18p6_exclude_1k_batch_2/Runs/ -sj
# python3 write_jsons.py /nesi/nobackup/nesi00213/RunFolder/Cybershake/v18p6_batched/v18p6_exclude_1k_batch_2/Runs/HopeCW -sf
# python3 write_jsons.py /nesi/nobackup/nesi00213/RunFolder/Cybershake/v18p6_batched/v18p6_exclude_1k_batch_2/Runs/HopeCW -sf -sj
"""

import os
import sys
import json
import glob
import argparse
import subprocess

from datetime import datetime
from multiprocessing import Pool

from qcore.utils import setup_dir

# the following keys should be consistent(in the same order) with the
# output params specified in proc_mpi_sl.template under each fault dir
# TODO auto retrive from the templates rather than hardcode
BB_KEYS = ["cores", "run_time", "fd_count", "dt", "start_time", "end_time"]
HF_KEYS = [
    "cores",
    "run_time",
    "fd_count",
    "nt",
    "nsub_stoch",
    "start_time",
    "end_time",
]
LF_KEYS = ["cores", "run_time", "nt", "nx", "ny", "nz", "start_time", "end_time"]
IM_KEYS = [
    "jobid",
    "cores",
    "fd_count",
    "nt",
    "run_time",
    "start_time",
    "end_time",
    "pSA_count",
    "im_components",
]
POST_EMOD_KEYS = ["start_time", "end_time"]
KEY_DICT = {
    "BB": BB_KEYS,
    "HF": HF_KEYS,
    "LF": LF_KEYS,
    "IM_calc": IM_KEYS,
    "post_emod": POST_EMOD_KEYS,
}

CH_LOG = "ch_log"
JSON_DIR = "jsons"
ALL_META_JSON = "all_sims.json"
PARAMS_BASE = "params_base.py"


def get_all_sims_dict(fault_dir, args):
    """
    :param fault_dir: abs path to a single fault dir
    :return: all_sims_dict or None
    """
    # Get all ch_log directories
    hh = get_hh(fault_dir)
    result_dict = {}

    # Find all ch_log and Rlog directories (to allow handling of the different
    # folder structures
    ch_log_dirs = glob.glob(os.path.join(fault_dir, "**", "ch_log"), recursive=True)
    rlog_dirs = glob.glob(
        os.path.join(fault_dir, "**", "LF", "**", "Rlog"), recursive=True
    )
    if len(ch_log_dirs) == 0:
        print("No ch_log directories found for {}".format(fault_dir))
        return

    for ch_log_dir in ch_log_dirs:
        # Iterate over the log files
        for f in os.listdir(ch_log_dir):
            segs = f.split(".")
            sim_type = segs[0]
            # exclude submit time log file (have 3 segs)
            if len(segs) == 4 and sim_type in KEY_DICT.keys():
                with open(os.path.join(ch_log_dir, f), "r") as log_file:
                    buf = log_file.readlines()

                raw_data = buf[0].strip().split()
                realization = raw_data[0]
                data = raw_data[2:]

                # Realization entry already exists
                if realization in result_dict.keys():
                    result_dict[realization].update({sim_type: {}})
                # Create the realization entry
                else:
                    result_dict[realization] = {"common": {"hh": hh}, sim_type: {}}

                # Retrieve the metadata
                for i in range(len(data)):
                    key = KEY_DICT[sim_type][i]
                    if key == "run_time":
                        data[i] = "{:.5f} hour".format(float(data[i]))
                    result_dict[realization][sim_type][key] = data[i]

                # Extra LF metadata
                if (
                    sim_type == "LF"
                    and result_dict[realization][sim_type].get("total_memory_usage")
                    is None
                ):
                    # Get the rlog dir for the current realization
                    cur_rlog_dir = [
                        rlog_dir
                        for rlog_dir in rlog_dirs
                        if realization in rlog_dir.split("/")
                    ]
                    cur_rlog_dir = None if len(cur_rlog_dir) == 0 else cur_rlog_dir[0]

                    result_dict[realization][sim_type][
                        "total_memory_usage"
                    ] = get_one_realization_memo_cores(cur_rlog_dir)

                result_dict[realization][sim_type]["submit_time"] = get_submit_time(
                    ch_log_dir, sim_type, realization, args
                )
    return result_dict


def get_one_realization_memo_cores(realization_rlog_dir):
    """
    :param realization_rlog_dir: **/LF/**/Rlog
    :return:total_memory usage for each realization in GB
    """
    os.chdir(realization_rlog_dir)
    cmd = "grep 'total for model' *.rlog"
    output = (
        subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True)
        .communicate()[0]
        .decode()
    )
    total = 0
    unit = ""

    for line in output.strip().split("\n"):
        memo, unit = line.strip().split("=")[1].strip().split()
        total += float(memo)
    if unit == "Mb":
        total /= 1024.0
        unit = "GB"
    return "{:.1f} {}".format(total, unit)


def get_hh(fault_dir):
    """
    :param fault_dir:
    :return: hh value string
    """
    os.chdir(fault_dir)
    if os.path.isfile(PARAMS_BASE):
        cmd = "grep 'hh =' params_base.py"

        # output = "hh = '0.4' #must be in formate
        # like 0.200 (3decimal places)\n"
        output = (
            subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True)
            .communicate()[0]
            .decode()
        )

        # "'0.4'" ---> '0.4'
        hh = output.split("#")[0].split("=")[1].strip().replace("'", "")
        return hh


def get_submit_time(ch_log_dir, sim_type, realization, args):
    """
    get submit_time for a realization by reading its log file
    :param sim_type: [BB|HF|LF]
    :param realization: eg.Hossack_HYP222-501_S3454
    :return: submit_time
    """
    if sim_type == "LF":
        sim_type = "EMOD3D"
    time_file = os.path.join(ch_log_dir, "{}.{}.log".format(sim_type, realization))
    if os.path.isfile(time_file):
        with open(time_file, "r") as f:
            submit_time = f.read().split(":")[-1].strip()  # 20181031_060458
        try:
            # 2018-10-31_06:59:22
            return datetime.strptime(submit_time, "%Y%m%d_%H%M%S").strftime(
                "%Y-%m-%d_%H:%M:%S"
            )
        except ValueError:
            return submit_time
    elif args.verbose:
        print(
            "{} {} does not have a submit time log file".format(sim_type, realization)
        )


def write_json(data_dict, out_dir, out_name):
    """writes a metadata json file"""
    json_data = json.dumps(data_dict)
    abs_outpath = os.path.join(out_dir, out_name)
    try:
        with open(abs_outpath, "w") as out_file:
            out_file.write(json_data)
    except Exception as e:
        sys.exit(e)


def write_fault_jsons(fault_dir, args):
    """write json file(s) for a single fault"""
    # Single JSON file per fault and it already exists,
    # skip the fault if --ignore_existing is set
    out_dir = os.path.join(fault_dir, JSON_DIR)
    if (
            args.single_json
            and args.ignore_existing
            and os.path.isfile(os.path.join(out_dir, ALL_META_JSON))
    ):
        if args.verbose:
            print("Skipped metadata collection " "for fault {}".format(fault_dir))
        return

    all_sims_dict = get_all_sims_dict(fault_dir, args)
    if all_sims_dict is not None:
        setup_dir(out_dir)

        if args.single_json:
            write_json(all_sims_dict, out_dir, ALL_META_JSON)
        else:
            for realization, realization_dict in all_sims_dict.items():
                out_name = "{}.json".format(realization)

                # Ignore if specified and file for realisation exists
                if args.ignore_existing and os.path.isfile(
                    os.path.join(out_dir, out_name)
                ):
                    if args.verbose:
                        print(
                            "Skipped realisation {} for "
                            "fault {}".format(realization, fault_dir)
                        )
                    continue

                write_json(realization_dict, out_dir, out_name)

        if args.verbose:
            print("Json file(s) written to {}".format(out_dir))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("run_folder", help="path to run_folder **/Runs/")
    parser.add_argument(
        "-sj",
        "--single_json",
        action="store_true",
        help="Please add '-sj' to indicate that you only want to output one "
        "single_json json file that contains all realizations. "
        "Default output one json file for each realization",
    )
    parser.add_argument(
        "-sf",
        "--single_fault",
        action="store_true",
        help="Please add '-sf' to indicate that run_folder path points to a "
        "single fault eg, add '-sf' if run_folder is '**/Runs/Hollyford'",
    )
    parser.add_argument(
        "--ignore_existing",
        action="store_true",
        default=False,
        help="If an output file already exists for a sepcific "
        "fault/realisation, then the fault/realisation is "
        "skipped.",
    )
    parser.add_argument(
        "--verbose", action="store_true", default=False, help="Add more verbosity."
    )
    parser.add_argument(
        "--n_procs", type=int, default=1, help="Number of processes to use."
    )

    args = parser.parse_args()

    # Check that the specified directory exists
    if not os.path.isdir(args.run_folder):
        parser.error("Folder {} does not exist".format(args.run_folder))

    # Single fault
    if args.single_fault:
        write_fault_jsons(args.run_folder, args)
    # Runs directory
    else:
        if args.n_procs == 1:
            for fault in os.listdir(args.run_folder):
                fault_dir = os.path.join(args.run_folder, fault)
                if os.path.isdir(fault_dir):
                    write_fault_jsons(fault_dir, args)
        else:
            p = Pool(args.n_procs)
            p.starmap(
                write_fault_jsons,
                [
                    (os.path.join(args.run_folder, fault), args)
                    for fault in os.listdir(args.run_folder)
                ],
            )


if __name__ == "__main__":
    main()
