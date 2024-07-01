"""
Collect estimated vs used CPU time for Cybershake-styled runs

This is particularly useful to accurately determine the estimated core hours for the entire set of Cybershake-styled
simulations. One can run the MEDIAN simulations only, and then run this script to collect the estimated core hours and
the cpu time actually used.
By multiplying the time_used by the number of realisations that is (optionally) supplied with --list argument,
we can accurately estimate the total core hours required to run the entire set of simulations.

This script is also useful to assess the quality of wall clock time estimation. If the estimated time is significantly
different from the time used, it may indicate that the wall clock time estimation method needs to be improved.

"""

import argparse
from datetime import datetime, timedelta
import json
import pandas as pd
from pathlib import Path
import sqlite3
import subprocess

from qcore.constants import ProcessType
from qcore import simulation_structure

PROCESS_TYPE = dict([(proc.value, proc.name) for proc in ProcessType])
REVERSE_PROCESS_TYPE = {name: value for value, name in PROCESS_TYPE.items()}

DEFAULT_OUTFILE = "est_vs_used_cpu_time.csv"
CONFIG_JSON = Path(__file__).parents[1] / "org/nesi/config.json"


def parse_args():
    parser = argparse.ArgumentParser(
        description="Get estimated vs used CPU time for jobs in the database"
    )
    parser.add_argument(
        "cs_root", type=Path, help="Path to the Cybershake root directory"
    )
    parser.add_argument("--list", type=Path, help="Path to the realization list file")
    parser.add_argument(
        "--update", type=Path, help="Path to the existing CSV file to update"
    )
    parser.add_argument(
        "--outfile",
        type=Path,
        help="Path to the output CSV file",
        default=DEFAULT_OUTFILE,
    )

    parser.add_argument(
        "--config", type=Path, help="Path to the config file", default=CONFIG_JSON
    )

    return parser.parse_args()


def proc_type_machine_mapping(config_file: Path) -> dict:
    """
    Read the config file and return the proc_type/machine mapping as a dictionary
    Parameters
    ----------
    config_file : Path
        Path to the config file

    Returns
    -------
    dict
        A dictionary of the proc_type: machine mapping

    """
    assert config_file.exists(), f"Config file {config_file} does not exist"
    with open(config_file, "r") as f:
        config_data = json.load(f)

    # Extract the MACHINE_TASKS dictionary
    proc_type_machine_dict = config_data.get("MACHINE_TASKS", {})

    return proc_type_machine_dict


def read_realization_list(list_file: Path) -> dict:
    """
    Read the realization list file and return a dictionary of event names and the number of realizations
    Parameters
    ----------
    list_file : Path
        Path to the realization list file

    Returns
    -------
    realization_data : dict
        A dictionary of event names and the number of realizations

    """
    realization_data = {}
    with open(list_file, "r") as f:
        for line in f:
            parts = line.strip().split()
            if len(parts) == 2:
                event_name, num_rels = parts
                realization_data[event_name] = int(
                    num_rels[:-1]
                )  # Remove the trailing 'r'
    return realization_data


def convert_time_used_to_seconds(time_used_str: str) -> float:
    """
    Convert time_used time from "hh:mm:ss" to total seconds
    Parameters
    ----------
    time_used_str : str
        time_used time in "hh:mm:ss" format
    Returns
    -------
    time_used_seconds : float
        time_used time in seconds
    """

    time_used_time = datetime.strptime(time_used_str, "%H:%M:%S")
    time_used_seconds = timedelta(
        hours=time_used_time.hour,
        minutes=time_used_time.minute,
        seconds=time_used_time.second,
    ).total_seconds()
    return time_used_seconds


def main():
    args = parse_args()
    args.cs_root.is_dir() or exit(f"{args.cs_root} is not a directory")
    if args.list is None:
        args.list = simulation_structure.get_cybershake_list(args.cs_root)

    # Read the config file and get the proc_type/machine mapping
    proc_type_machine_dict = proc_type_machine_mapping(args.config)

    # Connect to the SQLite database
    conn = sqlite3.connect(simulation_structure.get_mgmt_db(args.cs_root))
    cursor = conn.cursor()

    # Query the run data that has completed successfully
    cursor.execute(
        "SELECT run_name, proc_type, job_id FROM state WHERE status = 5 AND proc_type != 20"
    )  # status 5 is for the completed jobs. proc_type 20 is for NO_VM_PERT which has no job_id

    rows = cursor.fetchall()

    # Initialize a list to add the data to
    data_list = []

    # Read realization data from the list file
    realization_data = read_realization_list(args.list) if args.list.exists() else {}

    jobs_from_csv = {}
    existing_df = None

    if args.update is not None:
        # Read the existing CSV file
        args.update = Path(args.update)
        if args.update.exists():
            existing_df = pd.read_csv(args.update)
            jobs_from_csv = {
                (row["run_name"], REVERSE_PROCESS_TYPE[row["proc_type"]]): row["job_id"]
                for _, row in existing_df.iterrows()
            }
            print(
                f"Record found in {args.update} : {len(jobs_from_csv.keys())} entries"
            )

    jobs_from_db = {(row[0], row[1]): row[2] for row in rows}
    print(f"Record found in DB : {len(jobs_from_db.keys())} entries")

    jobs_to_add_dict = {
        (run_name, PROCESS_TYPE[proc_type]): jobs_from_db[(run_name, proc_type)]
        for run_name, proc_type in list(
            set(jobs_from_db.keys()) - set(jobs_from_csv.keys())
        )
    }
    print(f"Records to add to the CSV: {len(jobs_to_add_dict)}")

    # Iterate over job_ids
    for (run_name, proc_type_str), job_id in jobs_to_add_dict.items():

        assert job_id is not None, f"{run_name} {proc_type_str}"
        # Execute sacct command
        for machine in ["maui", "mahuika"]:
            cmd = f'sacct -j {job_id} -M {machine} --format="JobName,Elapsed,TimeLimit,AllocCPUS"'
            sacct_output = subprocess.check_output(
                cmd,
                shell=True,
            )
            sacct_lines = sacct_output.decode().splitlines()
            if len(sacct_lines) >= 3:
                break  # found the wanted result

        assert (
            len(sacct_lines) >= 3
        ), f"Something is wrong:  {job_id} {sacct_lines} {cmd}"  # make sure the command returned the output we want

        # Extract relevant info from the first row containing numbers
        job_info = sacct_lines[2].split()
        time_used, time_requested, num_cpus = job_info[1], job_info[2], job_info[3]

        # Determine if it's a MEDIAN event (based on run_name)
        is_median_event = "_REL" not in run_name  # otherwise, it is a realization
        num_rels = realization_data.get(run_name, 0) if is_median_event else 0
        num_rels = 0 if proc_type_str in ["VM_GEN", "INSTALL_FAULT"] else num_rels

        # Convert time_used time to seconds
        time_used_seconds = convert_time_used_to_seconds(time_used)

        # Calculate CPU-seconds used
        cpu_seconds_used = time_used_seconds * int(num_cpus)
        machine = proc_type_machine_dict[proc_type_str]
        data_list.append(
            {
                "run_name": run_name,
                "proc_type": proc_type_str,
                "machine": machine,
                "job_id": job_id,
                "num_cpus": num_cpus,
                "time_requested": time_requested,
                "time_used": time_used,
                "cpu_seconds_used": cpu_seconds_used,
                "num_rels": num_rels,
            }
        )

    # Create a DataFrame from the list of dictionaries
    df = pd.DataFrame(data_list)
    if existing_df is not None:
        updated_df = pd.concat([existing_df, df], ignore_index=True)
        # Sort by run_name and proc_type
        updated_df.sort_values(by=["run_name", "proc_type"], inplace=True)
        df = updated_df
    # Write to a CSV file
    df.to_csv(args.outfile, index=False)

    print(f"CSV file {args.outfile} created successfully!")


if __name__ == "__main__":
    main()
