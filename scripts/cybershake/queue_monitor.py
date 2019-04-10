#!/usr/bin/env python3
"""Script for continuously updating the slurm mgmt db from the queue."""
import signal
import os
import json
import argparse
import time
from typing import List

import qcore.simulation_structure as sim_struct
from scripts.management.MgmtDB import MgmtDB, SlurmTask


# Have to include sub-seconds, as clean up can run sub one second.
DATE_FORMAT = "%Y%m%d%H%M%S_%f"

terminate = False

def on_exit(signum, frame):
    global terminate
    terminate = True

def get_queue_entries(entry_files: List[str]):
    queue_entries = []
    for entry_file in entry_files:
        try:
            with open(entry_file, "r") as f:
                data_dict = json.load(f)
        except json.JSONDecodeError as ex:
            print(
                "Failed to decode the file {} as json. Check that this is "
                "valid json. Ignored!".format(
                    entry_file
                )
            )
            continue

        queue_entries.append(
            SlurmTask(
                run_name=os.path.basename(entry_file).split(".")[1],
                proc_type=data_dict[MgmtDB.col_proc_type],
                status=data_dict[MgmtDB.col_status],
                job_id=data_dict[MgmtDB.col_job_id],
                retries=data_dict[MgmtDB.col_retries],
                error=data_dict.get("error")
            )
        )

    return queue_entries


def main(args):
    mgmt_db = MgmtDB(sim_struct.get_mgmt_db(args.root_folder))
    queue_folder = sim_struct.get_mgmt_db_queue(args.root_folder)

    print("Running queue-monitor, exit with Ctrl-C.")
    while True:
        entry_files = os.listdir(queue_folder)
        entry_files.sort()

        entries = get_queue_entries(
            [os.path.join(queue_folder, file) for file in entry_files]
        )

        if len(entries) > 0:
            print("Updating {} mgmt db tasks.".format(len(entries)))
            if mgmt_db.update_entries_live(entries):
                print("Removing {} queue entry files".format(len(entry_files)))
                for file in entry_files:
                    os.remove(os.path.join(queue_folder, file))
            # Failed to update
            else:
                print(
                    "ERROR: Failed to update the current entries in the mgmt db queue. "
                    "Please investigate and fix. If this is a repeating error, then this "
                    "will block all other entries from updating."
                )
        else:
            print("No entries in the mgmt db queue.")

        if terminate:
            print("Exiting queue-monitor.")
            break

        # Nap time
        time.sleep(args.sleep_time)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("root_folder", type=str, help="Cybershake root folder.")
    parser.add_argument(
        "--sleep_time",
        type=int,
        help="Sleep time (in seconds) between queue checks.",
        default=5,
    )

    args = parser.parse_args()

    main(args)
