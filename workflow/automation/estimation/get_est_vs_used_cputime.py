"""
Collect estimated vs used CPU time for Cybershake-styled runs on NeSI (only works on Maui/Mahuika)

This is particularly useful to accurately estimate the core hours needed for the entire set of Cybershake-styled
simulations. One can run the MEDIAN simulations only, and then run this script to collect the estimated core hours and
the cpu time actually used.
By multiplying the time_used by the number of realisations,
we can accurately estimate the total core hours required to run the entire set of simulations.

This script is also useful to assess the quality of wall clock time estimation. If the estimated time is significantly
different from the time used, it may indicate that the wall clock time estimation method needs to be improved.

"""

import argparse
import json
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

from qcore import simulation_structure
from qcore.constants import ProcessType
from workflow.automation.lib import MgmtDB

PROCESS_TYPE = {proc.value: proc.name for proc in ProcessType}
REVERSE_PROCESS_TYPE = {name: value for value, name in PROCESS_TYPE.items()}

machines = ["maui", "mahuika"]
MEDIAN_ONLY_PROCS = ["VM_GEN", "INSTALL_FAULT", "VM_PARAMS"]
DEFAULT_OUTFILE = "est_vs_used_cpu_time.csv"
CONFIG_JSON = Path(__file__).parents[1] / "org/nesi/config.json"


def parse_args():
    parser = argparse.ArgumentParser(
        description="Get estimated vs used CPU time for jobs in the database"
    )
    parser.add_argument(
        "cs_root", type=Path, help="Path to the Cybershake root directory"
    )
    parser.add_argument("list", type=Path, help="Path to the realization list file")

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
    with MgmtDB.connect_db_ctx(simulation_structure.get_mgmt_db(args.cs_root)) as cur:
        # Query the run data that has completed successfully
        cur.execute(
            "SELECT run_name, proc_type, job_id FROM state WHERE status = 5 AND proc_type != 20"
        )  # status 5 is for the completed jobs. proc_type 20 is for NO_VM_PERT which has no job_id

        rows = cur.fetchall()

    jobs_from_db = {
        str(row[2]): (row[0], PROCESS_TYPE.get(row[1])) for row in rows
    }  # job_id : (run_name, proc_type_str) / ensure job_id is a str
    print(f"Record found in DB : {len(jobs_from_db)} entries")

    # Initialize a list to add the data to
    data_list = []

    # Read realization data from the list file
    assert args.list.exists()
    realization_numbers = read_realization_list(args.list)

    # initialize an empty list for each machine in machine_jobs dictionary
    machine_jobs = {machine: [] for machine in machines}

    # Iterate over job_ids collected from DB
    for job_id, (run_name, proc_type_str) in jobs_from_db.items():
        assert job_id is not None, f"{run_name} {proc_type_str}"
        machine = proc_type_machine_dict[proc_type_str]
        machine_jobs[machine].append(str(job_id))

    for machine in machines:
        job_ids = ",".join(machine_jobs[machine])
        cmd = (
            f'sacct -j {job_ids} -M {machine} --format="JobID,JobName,Elapsed,TimeLimit,AllocCPUS" -n '
            + "|awk '{$1=$1} NF==5'"
        )
        sacct_output = subprocess.check_output(
            cmd,
            shell=True,
        )
        sacct_lines = sacct_output.decode().splitlines()
        assert len(sacct_lines) == len(
            machine_jobs[machine]
        ), f"Something is wrong:  {len(sacct_lines)} {len(machine_jobs[machine])} {cmd}"  # make sure the command returned the output we want
        for line in sacct_lines:
            job_info = line.split()
            assert len(job_info) == 5
            job_id, _, time_used, time_requested, num_cpus = job_info
            assert job_id in machine_jobs[machine]
            run_name, proc_type_str = jobs_from_db[job_id]
            # Determine if it's a MEDIAN event (based on run_name)
            is_median_event = "_REL" not in run_name  # otherwise, it is a realization

            num_rels = realization_numbers.get(run_name, 0) if is_median_event else 0
            # we don't need to consider realisations for VM_GEN or INSTALL_FAULT
            num_rels = 0 if proc_type_str in MEDIAN_ONLY_PROCS else num_rels

            # Convert time_used time to seconds
            time_used_seconds = convert_time_used_to_seconds(time_used)

            # Calculate CPU-seconds used
            cpu_seconds_used = time_used_seconds * int(num_cpus)

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
                    "cpu_seconds_need_for_all_rels": cpu_seconds_used * num_rels,
                }
            )

    # Create a DataFrame from the list of dictionaries
    df = pd.DataFrame(data_list)
    df.sort_values(["run_name", "proc_type"], inplace=True)
    # Write to a CSV file
    df.to_csv(args.outfile, index=False)

    print(f"CSV file {args.outfile} created successfully!")


if __name__ == "__main__":
    main()
