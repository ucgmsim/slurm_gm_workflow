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
from logging import Logger

import pandas as pd
from filelock import SoftFileLock, Timeout

from qcore import utils
import qcore.constants as const
from qcore.srf import get_nsub_stoch
from qcore.qclogging import get_basic_logger

METADATA_VALUES = "metadata_values"
LOCK_FILENAME = "{}.lock".format(const.METADATA_LOG_FILENAME)

METACONST_TO_ADD = [const.MetadataField.run_time.value]


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
    sim_name: str = None,
    metaconst_to_add: List[str] = METACONST_TO_ADD,
    logger: Logger = get_basic_logger(),
):
    """Store metadata values in the specified json log file for a specific process type.

    Metadata values are stored as key, value pairs if a key already exists,
    it is not changed and instead new values are added with a postfix such as _1, _2, etc
    The exception are keys that are in the metaconst_to_add list, additional keys are
    also stored with a prefix, however their value is also added to the primary
    key (i.e. the one without a postfix). This is only allowed for values that can be
    converted to int or float.

    To prevent locking or any race condition the a lock for the log file is
    aquired at the start of the function and released at the end of it.
    The lock is kept for the full duration of the function and not just the read/write
    part as the file is overwritten and not updated.

    Parameters
    ----------
    log_file: str
        The absolute log file path
    proc_type: str
        The process type this is for, one of LF/HF/BB/IM
    metadata_dict: Dictionary with string keys and values
        The metadata key and value pairs
    sim_name: str
        The simulation/realisation name for the data provided is added
        to the json log file at the top level
    metaconst_to_add: List of strings
        Metadata keys for which their values are added (e.g. run_time)
    logger:
        Logger to pass log messages to. If None all messages will be printed to stdout or stderr depending on level
    """
    # Check that it is a valid process type
    if not const.ProcessType.has_str_value(proc_type):
        logger.warning("{} is not a valid process type. Logged anyway.".format(proc_type))

    lock_file = os.path.join(os.path.dirname(log_file), LOCK_FILENAME)
    lock = SoftFileLock(lock_file)

    # Attempt to acquire the lock
    try:
        lock.acquire(timeout=20)
    except Timeout:
        logger.error(
            "Failed to acquire the lock for the metadata log file, "
            "giving up on logging data. This should be investigated!"
            "The metadata that was unable to be logged is attached: {}".format(
                metadata_dict
            ),
        )
        return

    # Read the existing content if the log file exists
    json_data, proc_data = None, None
    if os.path.isfile(log_file):
        with open(log_file, "r") as f:
            json_data = json.load(f)
        proc_data = json_data.get(proc_type, None)
    # File doesn't exist yet
    else:
        json_data = {}

    # Add the simulation to the log file
    if (
        sim_name is not None
        and const.MetadataField.sim_name.value not in json_data.keys()
    ):
        json_data[const.MetadataField.sim_name.value] = sim_name
    elif (
        sim_name is not None
        and json_data.get(const.MetadataField.sim_name.value) != sim_name
    ):
        logger.warning(
            "Sim name {} does not match already existing sim name {} in metadata log file {}".format(
                sim_name, json_data.get(const.MetadataField.sim_name.value), log_file
            )
        )

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
                    logger.warning(
                        "Unsupported metadata value type for addition. "
                        "Check metadata values. "
                        "Value {} for key {} not added.".format(v, k),
                    )
                    continue
                else:
                    proc_data[k] = proc_data[k] + v

    # Write the json
    with open(log_file, "w") as f:
        json.dump(json_data, f)

    lock.release()


def main(args):
    # This should come from constants
    log_dir = os.path.join(args.sim_dir, "ch_log", const.METADATA_LOG_FILENAME)

    metadata_dict = getattr(args, METADATA_VALUES)

    # Determine run_time from start and end time
    if (
        const.MetadataField.start_time.value in metadata_dict.keys()
        and const.MetadataField.end_time.value in metadata_dict.keys()
    ):
        tdelta = datetime.strptime(
            metadata_dict[const.MetadataField.end_time.value], const.METADATA_TIMESTAMP_FMT
        ) - datetime.strptime(
            metadata_dict[const.MetadataField.start_time.value], const.METADATA_TIMESTAMP_FMT
        )
        metadata_dict[const.MetadataField.run_time.value] = tdelta.total_seconds() / 3600

    # Load the params
    params = utils.load_sim_params(
        os.path.join(args.sim_dir, "sim_params.yaml"), load_vm=True
    )

    # params metadata for LF
    if args.proc_type == const.ProcessType.EMOD3D.str_value:
        metadata_dict[const.MetadataField.nt.value] = int(
            float(params.sim_duration) / float(params.dt)
        )
        metadata_dict[const.MetadataField.nx.value] = params.nx
        metadata_dict[const.MetadataField.ny.value] = params.ny
        metadata_dict[const.MetadataField.nz.value] = params.nz
    # HF
    elif args.proc_type == const.ProcessType.HF.str_value:
        metadata_dict[const.MetadataField.nt.value] = int(
            float(params.sim_duration) / float(params.hf.dt)
        )
        metadata_dict[const.MetadataField.nsub_stoch.value] = get_nsub_stoch(
            params["hf"]["slip"], get_area=False
        )
    # BB
    elif args.proc_type == const.ProcessType.BB.str_value:
        metadata_dict[const.MetadataField.dt.value] = params.hf.dt
    # IM_calc
    elif args.proc_type == const.ProcessType.IM_calculation.str_value:
        metadata_dict[const.MetadataField.nt.value] = int(
            float(params.sim_duration) / float(params.hf.dt)
        )
        # This should come from a constants file
        im_calc_csv_file = os.path.join(
            args.sim_dir, "IM_calc", "{}.csv".format(os.path.basename(args.sim_dir))
        )
        im_comp = list(pd.read_csv(im_calc_csv_file).component.unique().astype("U"))

        metadata_dict[const.MetadataField.im_comp.value] = im_comp
        metadata_dict[const.MetadataField.im_comp_count.value] = len(im_comp)

    store_metadata(
        log_dir, args.proc_type, metadata_dict, sim_name=os.path.basename(args.sim_dir)
    )


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
