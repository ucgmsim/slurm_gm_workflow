#!/usr/bin/env python3

"""
Scrapes the HF logs passed in via a glob string and calculates the amount of core hours lost to thread idling
Takes in a single or glob path to HF log files.
"""

import argparse
from datetime import datetime
from glob import glob

import numpy as np


def get_duration_and_dead_ratio(file_name):
    """
    Takes a log filename and extracts the amount of wasted core hours.
    :returns: A tuple containing the sim duration, percent of time lost, number of cores with valid times and the time lost for each core
    """
    with open(file_name) as f:
        lines = f.readlines()
    final_times = {}
    j = 1
    release = lines[-j].split(":")
    while (
        len(release) < 6
        or not release[5].startswith("Simulation completed.")
        and j < len(lines)
    ):
        j += 1
        release = lines[-j].split(":")
    try:
        assert j < len(lines)
        end_time = datetime.strptime(":".join(release[:3]), "%Y-%m-%d %H:%M:%S,%f")
    except:
        if verbose:
            print(file_name, "Couldn't get a start time")
        return None, None, None, None
    for line in lines[-(j + 1) :: -1]:
        line = line.split(":")
        if len(line) < 6 or not line[5].startswith("Process "):
            continue
        try:
            time = datetime.strptime(":".join(line[:3]), "%Y-%m-%d %H:%M:%S,%f")
        except:
            continue
        rank = line[3].split("_")[-1]
        if rank not in final_times.keys():
            final_times.update({rank: time})
    if len(final_times) < 2:
        if verbose:
            print(file_name, "Not enough times")
        return None, None, None, None
    times = sorted(final_times.values())
    first_time = times[0]
    duration = (end_time - first_time).total_seconds()
    total_dead_time = 0
    lost_times = []
    for time in times:
        lost_times.append((end_time - time).total_seconds())
        if very_verbose:
            print("adding {}".format((end_time - time).total_seconds() / duration))
        total_dead_time += (end_time - time).total_seconds() / duration
    total_dead_time *= 1 / len(times)
    return duration, total_dead_time, len(times), lost_times


parser = argparse.ArgumentParser()
parser.add_argument(
    "log_glob", help='log file selection expression. eg: "Runs/*/*/HF/Acc/HF.log"'
)
parser.add_argument(
    "-o", "--out_file", help="The file to write the data to", default="hf_ch_burn.csv"
)
parser.add_argument("-v", "--verbose", help="Additional print statements enabled.")
parser.add_argument(
    "-vv",
    "--very_verbose",
    help="Even more print statements enabled. Intended for use with a single realisation, or part log.",
)
args = parser.parse_args()
log_files = glob(args.log_glob)

verbose = args.verbose or args.very_verbose
very_verbose = args.very_verbose

rels = len(log_files)

values = np.ndarray(
    rels,
    dtype=[
        ("f_name", "U128"),
        ("fault", "U32"),
        ("duration", "f8"),
        ("efficiency", "f8"),
        ("ch_burned", "f8"),
    ],
)

for i, file in enumerate(log_files):
    (
        dead_duration,
        decimal_dead_time,
        node_count,
        dead_times,
    ) = get_duration_and_dead_ratio(file)
    if dead_duration is None:
        dead_times = [0]
    parts = file.split("/")
    runs_idex = parts.index("Runs")
    fault = parts[runs_idex + 1]
    if node_count is not None and node_count % 40 != 0:
        if verbose:
            print(file, "Nodes off: {}".format(node_count))
        extra_nodes = np.ceil(node_count / 80) * 80
        dead_times.extend(
            [np.mean(dead_times) for i in range(extra_nodes - node_count)]
        )
    if verbose:
        print(
            file,
            dead_duration,
            sum(dead_times) / 3600,
            node_count * dead_duration * (decimal_dead_time / 3600),
        )
    values[i] = (file, fault, dead_duration, decimal_dead_time, sum(dead_times) / 3600)

faults = np.unique(values["fault"])

for fault in faults:
    fault_mask = values["fault"] == fault
    avg = np.mean(values[fault_mask]["duration"])
    stdev = np.std(values[fault_mask]["duration"])
    outliers = values["duration"] > avg * 2
    values = values[~np.logical_and(fault_mask, outliers)]
    if sum(np.logical_and(fault_mask, outliers)) > 0:
        print(
            "Removing {} outliers for fault {}".format(
                sum(np.logical_and(fault_mask, outliers)), fault
            )
        )

np.savetxt(args.out_file, values, delimiter=", ", fmt="%s")
values = values[values["ch_burned"] > 0]
print("Average duration: {}s".format(sum(values["duration"]) / len(values)))
print("Average ch burned: {}".format(sum(values["ch_burned"]) / len(values) / 2))
print("Total known ch burned: {}".format(sum(values["ch_burned"]) / 2))
print(
    "Extrapolated ch burned: {}".format(
        sum(values["ch_burned"]) / len(values) / 2 * rels
    )
)
