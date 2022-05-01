#!/usr/bin/env python3
"""This script is used after the workflow has been run to collect metadata and compile it all to a csv
Example:
python3 log_metadata.py ./log_dir LF cores=12 run_time=12.5
"""
import argparse

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
'00" | sed \'s/\'//g\n\n