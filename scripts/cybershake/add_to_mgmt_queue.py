#!/usr/bin/env python3
"""Wrapper script used by the templates to add updates to the mgmt db queue"""
import argparse

import qcore.constants as const
from shared_workflow import shared

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("queue_folder", type=str, help="Mgmt db queue folder")
    parser.add_argument("run_name", type=str, help="The realisation/run name")
    parser.add_argument(
        "proc_type", type=str, help="The string value of the process type."
    )
    parser.add_argument("status", type=int, help="The integer value for the status")
    parser.add_argument(
        "--error",
        type=str,
        help="Errors that occurred during the execution of the script.",
    )

    args = parser.parse_args()
    shared.add_to_queue(
        args.queue_folder, args.run_name,
        const.ProcessType.from_str(args.proc_type).value, args.status, error=args.error
    )
