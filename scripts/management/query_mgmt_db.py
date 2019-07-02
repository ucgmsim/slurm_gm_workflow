"""
@author: jam335 - jason.motha@canterbury.ac.nz

A script that queries a slurm mgmt db and returns the status of a task
"""

import argparse
import scripts.management.db_helper as db_helper
from shared_workflow.shared_automated_workflow import parse_config_file


def print_run_status(db, run_name, error=False, count=False, config_file=None):
    if error:
        db.execute(
            """SELECT state.run_name, proc_type_enum.proc_type, status_enum.state, state.job_id, datetime(last_modified,'unixepoch'), error.error
                    FROM state, status_enum, proc_type_enum, error
                    WHERE state.proc_type = proc_type_enum.id 
                    AND state.status = status_enum.id
                    AND UPPER(state.run_name) LIKE UPPER(?) 
                    AND error.task_id = state.id
                    ORDER BY state.run_name, status_enum.id
                    """,
            (run_name,),
        )
        status = db.fetchall()
        for statum in status:
            print(
                """ Run_name: {}\n Process: {}\n Status: {}\n Job-ID: {}\n Last_Modified: {}\n Error: {} \n""".format(
                    *statum
                )
            )
    elif count:
        vals = []
        for i in range(1, 7):
            vals.append(
                db.execute(
                    """SELECT COUNT(*)
                        FROM state
                        WHERE status = ?
                        """,
                    (i,),
                ).fetchone()[0]
            )
        print(
            "created: {}, queued: {}, running: {}, completed: {}, failed: {}, other: {}, total: {}".format(
                *vals, sum(vals)
            )
        )
    else:
        if config_file is not None:
            tasks_n, tasks_to_match = parse_config_file(config_file)
            status = []
            if len(tasks_n) > 0:
                status.extend(
                    db.execute(
                        """SELECT state.run_name, proc_type_enum.proc_type, status_enum.state, state.job_id, datetime(last_modified,'unixepoch')
                            FROM state, status_enum, proc_type_enum
                            WHERE state.proc_type = proc_type_enum.id 
                            AND state.status = status_enum.id
                            AND state.proc_type IN (?{})
                            ORDER BY state.run_name, status_enum.id
                            """.format(
                            ",?" * (len(tasks_n) - 1)
                        ),
                        [i.value for i in tasks_n],
                    ).fetchall()
                )
            for pattern, tasks in tasks_to_match:
                tasks = [i.value for i in tasks]
                status.extend(
                    db.execute(
                        """SELECT state.run_name, proc_type_enum.proc_type, status_enum.state, state.job_id, datetime(last_modified,'unixepoch')
                    FROM state, status_enum, proc_type_enum
                    WHERE state.proc_type = proc_type_enum.id 
                    AND state.status = status_enum.id
                    AND state.run_name LIKE ?
                    AND state.proc_type IN (?{})
                    AND 
                    ORDER BY state.run_name, status_enum.id
                    """.format(
                            ",?" * (len(tasks) - 1)
                        ),
                        (pattern, *tasks),
                    ).fetchall()
                )
        else:
            db.execute(
                """SELECT state.run_name, proc_type_enum.proc_type, status_enum.state, state.job_id, datetime(last_modified,'unixepoch')
                        FROM state, status_enum, proc_type_enum
                        WHERE state.proc_type = proc_type_enum.id 
                        AND state.status = status_enum.id
                        AND UPPER(state.run_name) LIKE UPPER(?)
                        ORDER BY state.run_name, status_enum.id
                        """,
                (run_name,),
            )
            status = db.fetchall()
        print(
            "{:>25} | {:>15} | {:>10} | {:>8} | {:>20}".format(
                "run_name", "process", "status", "job-id", "last_modified"
            )
        )
        print("_" * (25 + 15 + 10 + 20 + 8 + 7 + 3 * 4))
        for statum in status:
            print("{:>25} | {:>15} | {:>10} | {!s:>8} | {:>20}".format(*statum))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "run_folder", type=str, help="folder to the collection of runs on Kupe"
    )
    parser.add_argument(
        "run_name", type=str, nargs="?", default="%", help="name of run to be queried"
    )
    parser.add_argument(
        "--error",
        "-e",
        action="store_true",
        help="Optionally add an error string to the database",
    )
    parser.add_argument(
        "--count",
        "-c",
        action="store_true",
        help="Get counts for each possible state. Does nothing if --error is given",
    )
    parser.add_argument(
        "--config",
        type=str,
        default=None,
        help="The cybershake config file defining which tasks are being run, and should be looked at ",
    )

    args = parser.parse_args()
    f = args.run_folder
    run_name = args.run_name
    error = args.error
    db = db_helper.connect_db(f)

    print_run_status(db, run_name, error, args.count, args.config)


if __name__ == "__main__":
    main()
