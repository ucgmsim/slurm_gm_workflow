#!/usr/bin/env python3
"""This script is used from inside the submit/run slurm scripts to store metadata in a
json file.

Example:
python3 log_metadata.py ./log_dir LF cores=12 run_time=12.5
"""
import os
import sys
import json
import argparse
from typing import Dict, List
from datetime import datetime

from metadata.agg_json_data import ProcTypeConst, MetaConst
from qcore import utils
from qcore.srf import get_nsub_stoch

METADATA_VALUES = "metadata_values"
LOG_FILENAME = "metadata_log.json"

METACONST_TO_ADD = [MetaConst.run_time.value]
TIMESTAMP_FMT = "%Y-%m-%d_%H:%M:%S"


class KeyValuePairsAction(argparse.Action):
    """Allows passing key=value pairs to the parser."""

    def __call__(self, parser, namespace, values, option_string=None):
        values_dict = {}
        for value in values:
            if "=" not in value:
                print(
                    "The metadata key=value, {}, was in the incorrect format. "
                    "Not written!".format(value),
                    file=sys.stderr,
                )
                continue
            k, v = value.split("=")
            if k in values_dict.keys():
                print(
                    "The metadata key {} already exists. There should never be any"
                    "duplicate metadata key=value pairs. "
                    "Pair {}, {} skipped!".format(k, k, v)
                )
                continue
            values_dict[k] = v
        setattr(namespace, METADATA_VALUES, values_dict)


def convert_to_numeric(str_value):
    """Attempts to convert the string value to an integer or float"""
    try:
        return int(str_value)
    except ValueError:
        try:
            return float(str_value)
        except ValueError:
            pass

    return str_value


def store_metadata(
    log_file: str,
    proc_type: str,
    metadata_dict: Dict[str, str],
    metaconst_to_add: List[str] = METACONST_TO_ADD,
):
    """Store metadata values in the specified json log file for a specific process type.

    Metadata values are stored as key, value pairs if a key already exists,
    it is not changed and instead new values are added with a postfix such as _1, _2, etc
    The exception are keys that are in the metaconst_to_add list, additional keys are
    also stored with a prefix, however their value is also added to the primary
    key (i.e. the one without a postfix). This is only allowed for values that can be
    converted to int or float.

    Parameters
    ----------
    log_file: str
        The absolute log file path
    proc_type: str
        The process type this is for, one of LF/HF/BB/IM
    metadata_dict: Dictionary with string keys and values
        The metadata key and value pairs
    metaconst_to_add: List of strings
        Metadata keys for which their values are added (e.g. run_time)
    """
    # Check that it is a valid process type
    if not ProcTypeConst.has_value(proc_type):
        print("{} is not a valid process type. Logged anyways.".format(proc_type))

    # Read the existing content if the log file exists
    json_data, proc_data = None, None
    if os.path.isfile(log_file):
        with open(log_file, "r") as f:
            json_data = json.load(f)
        proc_data = json_data.get(proc_type, None)
    # File doesn't exist yet
    else:
        json_data = {}

    if proc_data is None:
        proc_data = {}
        json_data[proc_type] = proc_data

    for k, v in metadata_dict.items():
        if type(v) is str:
            v = convert_to_numeric(v)

        # Key doesn't exists yet
        if k not in proc_data.keys():
            proc_data[k] = v
            continue

        # Key already exists
        if k in proc_data.keys():
            k_count = sum([1 for cur_k in proc_data.keys() if k in cur_k])

            # Key has only been added once before (i.e. primary value)
            # Duplicate and add _1 postfix
            if k_count == 1:
                proc_data["{}_1".format(k)] = proc_data[k]

                # Add new value
                proc_data["{}_2".format(k)] = v
            # Several keys already exists, just add additional with count postfix
            else:
                # Don't need a +1 as the count includes the primary value
                proc_data["{}_{}".format(k, k_count)] = v

            # Some additional values are required to be added to the existing
            # value (e.g. run_time)
            if k in metaconst_to_add:
                if type(v) is not int and type(v) is not float:
                    print(
                        "Unsupported metadata value type for addition. "
                        "Check metadata values. "
                        "Value {} for key {} not added.".format(v, k),
                        file=sys.stderr,
                    )
                    continue
                else:
                    proc_data[k] = proc_data[k] + v

    # Write the json
    with open(log_file, "w") as f:
        json.dump(json_data, f)


def main(args):
    # This should come from constants
    log_dir = os.path.join(args.sim_dir, "ch_log", LOG_FILENAME)

    metadata_dict = getattr(args, METADATA_VALUES)

    # Determine run_time from start and end time
    if (
        MetaConst.start_time.value in metadata_dict.keys()
        and MetaConst.end_time.value in metadata_dict.keys()
    ):
        tdelta = datetime.strptime(
            metadata_dict[MetaConst.end_time.value], TIMESTAMP_FMT
        ) - datetime.strptime(metadata_dict[MetaConst.start_time.value], TIMESTAMP_FMT)
        metadata_dict[MetaConst.run_time.value] = tdelta.total_seconds() / 3600

    # params metadata for LF
    if args.proc_type == ProcTypeConst.LF.value:
        params = utils.load_sim_params(
            os.path.join(args.sim_dir, "sim_params.yaml"), load_vm=True
        )
        metadata_dict[MetaConst.nt.value] = int(
            float(params.sim_duration) / float(params.dt)
        )
        metadata_dict[MetaConst.nx.value] = params.nx
        metadata_dict[MetaConst.ny.value] = params.ny
        metadata_dict[MetaConst.nz.value] = params.nz
    # HF
    elif args.proc_type == ProcTypeConst.HF.value:
        params = utils.load_sim_params(
            os.path.join(args.sim_dir, "sim_params.yaml"), load_vm=False
        )
        metadata_dict[MetaConst.nt.value] = int(
            float(params.sim_duration) / float(params.hf.hf_dt)
        )
        metadata_dict[MetaConst.nsub_stoch.value] = get_nsub_stoch(
            params["hf"]["hf_slip"], get_area=False
        )
    # BB
    elif args.proc_type == ProcTypeConst.BB.value:
        params = utils.load_sim_params(
            os.path.join(args.sim_dir, "sim_params.yaml"), load_vm=False
        )
        metadata_dict[MetaConst.dt.value] = params.hf.hf_dt

    store_metadata(log_dir, args.proc_type, metadata_dict)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("sim_dir", type=str, help="The log directory")
    parser.add_argument(
        "proc_type",
        type=str,
        help="The process type to log metadata for. Has to be one of LF/HF/BB/IM",
    )
    parser.add_argument(
        METADATA_VALUES,
        action=KeyValuePairsAction,
        nargs="+",
        help="The key=value pairs (no space), pairs separated by space",
    )

    args = parser.parse_args()

    main(args)
