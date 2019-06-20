#!/usr/bin/env python3
"""Wrapper script used by the templates to add updates to the mgmt db queue"""
import argparse

import qcore.constants as const
from shared_workflow.shared_automated_workflow import add_to_queue

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("queue_folder", type=str, help="Mgmt db queue folder")
    parser.add_argument("run_name", type=str, help="The realisation/run name")
    parser.add_argument(
        "proc_type",
        type=str,
        help="The string value of the process type.",
        choices=list(const.ProcessType.iterate_str_values()),
    )
    parser.add_argument(
        "status",
        type=str,
        help="The string value of the status",
        choices=list(const.Status.iterate_str_values()),
    )
    parser.add_argument(
        "--error",
        type=str,
        help="Errors that occurred during the execution of the script.",
    )

    args = parser.parse_args()
    add_to_queue(
        args.queue_folder,
        args.run_name,
        const.ProcessType.from_str(args.proc_type).value,
        const.Status.from_str(args.status).value,
        error=args.error,
    )
