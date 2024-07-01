import sqlite3
import subprocess
import pandas as pd
import argparse
from qcore.constants import ProcessType
from datetime import datetime, timedelta
from pathlib import Path

PROCESS_TYPE = dict([(proc.value, proc.name) for proc in ProcessType])
REVERSE_PROCESS_TYPE = {name: value for value, name in PROCESS_TYPE.items()}

DEFAULT_OUTCSV = "est_vs_used_cpu_time.csv"


def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate CSV data from SQLite and SLURM"
    )
    parser.add_argument("--list", help="Path to the realization list file")
    parser.add_argument("--update", help="Path to the existing CSV file to update")
    parser.add_argument(
        "--outfile", help="Path to the output CSV file", default=DEFAULT_OUTCSV
    )

    return parser.parse_args()


def read_realization_list(list_file):
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


def convert_elapsed_to_seconds(elapsed_str):
    # Convert elapsed time from "hh:mm:ss" to total seconds
    elapsed_time = datetime.strptime(elapsed_str, "%H:%M:%S")
    elapsed_seconds = timedelta(
        hours=elapsed_time.hour,
        minutes=elapsed_time.minute,
        seconds=elapsed_time.second,
    ).total_seconds()
    return elapsed_seconds


def main():
    args = parse_args()

    # Connect to the SQLite database
    conn = sqlite3.connect("slurm_mgmt.db")
    cursor = conn.cursor()

    # Query the relevant data
    cursor.execute(
        "SELECT run_name, proc_type, job_id FROM state WHERE status = 5 AND proc_type != 20"
    )
    rows = cursor.fetchall()

    # Initialize a list to add
    data_list = []

    # Read realization data from the list file
    realization_data = read_realization_list(args.list) if args.list else {}

    jobs_from_csv = {}
    existing_df = None

    if args.update is not None:
        args.update = Path(args.update)
        if args.update.exists():
            existing_df = pd.read_csv(args.update)
            jobs_from_csv = {
                (row["run_name"], REVERSE_PROCESS_TYPE[row["proc_type"]]): row["job_id"]
                for _, row in existing_df.iterrows()
            }

    jobs_from_db = {(row[0], row[1]): row[2] for row in rows}
    print(f"Record found in {args.update} : {len(jobs_from_csv.keys())} entries")
    print(f"Record found in DB : {len(jobs_from_db.keys())} entries")

    jobs_to_add_dict = {
        (run_name, PROCESS_TYPE[proc_type]): jobs_from_db[(run_name, proc_type)]
        for run_name, proc_type in list(
            set(jobs_from_db.keys()) - set(jobs_from_csv.keys())
        )
    }

    print(f"Records to add to the CSV: {jobs_to_add_dict}")

    # Iterate over job_ids
    #    for run_name, proc_type_str,job_id in jobs_to_add_dict.items():
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
        ), f"{job_id} {sacct_lines} {cmd}"  # make sure the command returned the output we want

        # Extract relevant info from the first row containing numbers
        try:
            job_info = sacct_lines[2].split()
        except IndexError:
            raise
        elapsed, timelimit, alloc_cpus = job_info[1], job_info[2], job_info[3]

        # Determine if it's an event (based on run_name)
        is_event = "_REL" not in run_name  # otherwise, it is a realization
        num_rels = realization_data.get(run_name, None) if is_event else None

        # Convert elapsed time to seconds
        elapsed_seconds = convert_elapsed_to_seconds(elapsed)

        # Calculate Corehours_used
        coreseconds_used = elapsed_seconds * int(alloc_cpus)

        data_list.append(
            {
                "run_name": run_name,
                "proc_type": proc_type_str,
                "job_id": job_id,
                "CPUs_used": alloc_cpus,
                "Time_requested": timelimit,
                "Time_used": elapsed,
                "Coreseconds_used": coreseconds_used,
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
