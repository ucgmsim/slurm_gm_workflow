"""
@author: jam335 - jason.motha@canterbury.ac.nz

A script that queries a slurm mgmt db and returns the status of a task
"""

import argparse
from typing import Union, List

from workflow.automation.lib import MgmtDB

from workflow.automation.lib.shared_automated_workflow import parse_config_file

PATTERN_FORMATTER = "{:>25}, {:>15}: created: {:>5}, queued: {:>5}, running: {:>5}, other: {:>5}, completed: {:>5}, failed: {:>5}, total: {:>6}"
PATTERN_TODO_FORMATTER = "{:>25}, {:>15}: created: {:>5}"
RETRY_MAX_FILTER = """AND (SELECT count(*) 
                    FROM state as state2, status_enum as status_enum2 
                    WHERE state2.status = status_enum2.id 
                    AND status_enum2.state <> 'failed' 
                    AND state2.run_name = s.run_name
                    AND s.proc_type = state2.proc_type)
                 = 0"""


class QueryModes:
    def __init__(
        self,
        error=False,
        count=False,
        todo=False,
        retry_max=False,
        detailed_count=False,
    ):
        self.error = error
        self.count = count
        self.todo = todo
        self.retry_max = retry_max
        self.detailed_count = detailed_count


def state_table_query_builder(
    what: Union[str, List[str]],
    state: Union[bool, int] = False,
    process_type: Union[bool, int] = False,
    run_name_type: MgmtDB.ComparisonOperator = None,
    task_id: Union[bool, int] = False,
    ordering: Union[bool, str, List[str]] = False,
):
    """
    Creates queries for the state table.
    For data sanitation reasons variables are passed in separately to the query string (No Bobby tables here)
    :param what: The columns to be selected
    :param state: Either true for one, or the number of states to be selected
    :param process_type: Either true for one, or the number of states to be selected
    :param run_name_exact: If an exact pattern match is required
    :param run_name_similar: If a pattern match is required, to be used with '%' as the sqlite wildcard character
    :param run_name_disimilar: If a pattern non-match is required, to be used with '%' as the sqlite wildcard character
    :param task_id: Either true for one, or the number of states to be selected
    :param ordering: The columns to order the results by
    :return: The string to be used as the query
    """
    if isinstance(what, str):
        what = [what]
    query = f"SELECT {', '.join(what)} FROM state "

    wheres = []
    if run_name_type is not None:
        wheres.append(f"run_name {run_name_type.value} ?")

    if state is not False:
        if state is not True:
            wheres.append(f"status in (?{',?'*(state-1)})")
        else:
            wheres.append(f"status = ?")
    if task_id is not False:
        if task_id is not True:
            wheres.append(f"task_id in (?{',?'*(task_id-1)})")
        else:
            wheres.append("task_id = ?")
    if process_type is not False:
        if process_type is not True:
            wheres.append(f"proc_type in (?{',?'*(process_type-1)})")
        else:
            wheres.append(f"proc_type = ?")

    if len(wheres) > 0:
        query = query + "Where " + " AND ".join(wheres)

    if ordering is not False:
        if isinstance(ordering, str):
            ordering = [ordering]
        query = query + f" ORDER BY {', '.join(list(ordering))}"
    return query


def print_run_status(db, run_name, query_mode: QueryModes, config_file=None):
    if query_mode.error:
        show_all_error_entries(db, run_name, query_mode.retry_max)
    elif query_mode.count:
        if query_mode.detailed_count:
            if config_file is not None:
                show_detailed_config_counts(config_file, db, query_mode.todo)
            else:
                show_state_counts(db, query_mode.todo)
        else:
            if config_file is not None:
                show_pattern_state_counts(config_file, db, query_mode.todo)
            else:
                show_state_counts(db, query_mode.todo)
    else:
        if config_file is not None:
            status = get_all_entries_from_config(config_file, db, query_mode)
        else:
            status = get_all_entries(db, run_name, query_mode)
        print(
            "{:>25} | {:>15} | {:>10} | {:>8} | {:>20}".format(
                "run_name", "process", "status", "job-id", "last_modified"
            )
        )
        print("_" * (25 + 15 + 10 + 20 + 8 + 7 + 3 * 4))
        for statum in status:
            print("{:>25} | {:>15} | {:>10} | {!s:>8} | {:>20}".format(*statum))


def get_all_entries(db, run_name, query_mode):
    extra_query = ""
    if query_mode.todo:
        extra_query = """AND NOT EXISTS (
        SELECT 1 FROM state s_inner
        WHERE s_inner.run_name = state.run_name AND s_inner.proc_type = state.proc_type AND s_inner.status = 5)
        AND (s.job_id, s.last_modified) IN (
        SELECT MAX(job_id), MAX(last_modified) FROM state WHERE run_name = s.run_name AND proc_type = s.proc_type
        )
        """
    elif query_mode.retry_max:
        extra_query = RETRY_MAX_FILTER
    base_command = f"""SELECT s.run_name, pe.proc_type, se.state, s.job_id, datetime(last_modified,'unixepoch') lm_time
                        FROM state s
                        JOIN status_enum se ON s.status = se.id
                        JOIN proc_type_enum pe ON s.proc_type = pe.id
                        WHERE
                        UPPER(s.run_name) LIKE UPPER(?)
                        {extra_query}
                        ORDER BY s.run_name, pe.proc_type, se.state"""

    db.execute(
        base_command,
        (run_name,),
    )
    status = db.fetchall()
    return status


def get_all_entries_from_config(config_file, db, query_mode):
    extra_query = ""
    if query_mode.todo:
        extra_query = """AND NOT EXISTS (
        SELECT 1 FROM state s_inner
        WHERE s_inner.run_name = s.run_name AND s_inner.proc_type = s.proc_type AND s_inner.status = 5)
        AND (s.job_id, s.last_modified) IN (
        SELECT MAX(job_id), MAX(last_modified) FROM state WHERE run_name = s.run_name AND proc_type = s.proc_type
        )
        """
    elif query_mode.retry_max:
        extra_query = RETRY_MAX_FILTER
    tasks_n, tasks_to_match, tasks_to_not_match = parse_config_file(config_file)
    status = []

    base_command = f""" SELECT s.run_name, pe.proc_type, se.state, s.job_id, datetime(last_modified,'unixepoch') lm_time
                        FROM state s
                        JOIN status_enum se ON s.status = se.id
                        JOIN proc_type_enum pe ON s.proc_type = pe.id
                        WHERE
                        {{}}
                        AND s.proc_type IN (?{{}})
                        {extra_query}
                        ORDER BY s.run_name, pe.proc_type, se.state"""

    if len(tasks_n) > 0:
        status.extend(
            db.execute(
                base_command.format("", ",?" * (len(tasks_n) - 1)),
                [i.value for i in tasks_n],
            ).fetchall()
        )
    for pattern, tasks in tasks_to_match:
        tasks = [i.value for i in tasks]
        status.extend(
            db.execute(
                base_command.format(
                    "s.run_name LIKE ?", ",?" * (len(tasks) - 1)
                ),
                (pattern, *tasks),
            ).fetchall()
        )
    for pattern, tasks in tasks_to_not_match:
        tasks = [i.value for i in tasks]
        status.extend(
            db.execute(
                base_command.format(
                    "AND s.run_name NOT LIKE ?", ",?" * (len(tasks) - 1)
                ),
                (pattern, *tasks),
            ).fetchall()
        )
    return status


def show_pattern_state_counts(config_file, db, todo=False):
    tasks_n, tasks_to_match, tasks_to_not_match = parse_config_file(config_file)
    vals = []
    for i in range(1, 7):
        vals.append(
            db.execute(
                state_table_query_builder("COUNT(*)", state=True), (i,)
            ).fetchone()[0]
        )
    if todo:
        print(PATTERN_TODO_FORMATTER.format("ALL", "ALL", vals[0]))
    else:
        print(PATTERN_FORMATTER.format("ALL", "ALL", *vals, sum(vals)))
    for pattern, tasks in tasks_to_match:
        vals = []
        for i in range(1, 7):
            vals.append(
                db.execute(
                    state_table_query_builder(
                        "COUNT(*)",
                        state=True,
                        run_name_type=MgmtDB.ComparisonOperator.LIKE,
                    ),
                    (pattern, i),
                ).fetchone()[0]
            )
        if todo:
            print(PATTERN_TODO_FORMATTER.format("ALL", "ALL", vals[0]))
        else:
            print(
                PATTERN_FORMATTER.format(
                    ", ".join([task.name for task in tasks]), pattern, *vals, sum(vals)
                )
            )
    for pattern, tasks in tasks_to_not_match:
        vals = []
        for i in range(1, 7):
            vals.append(
                db.execute(
                    state_table_query_builder(
                        "COUNT(*)",
                        state=True,
                        run_name_type=MgmtDB.ComparisonOperator.NOTLIKE,
                    ),
                    (pattern, i),
                ).fetchone()[0]
            )
        if todo:
            print(PATTERN_TODO_FORMATTER.format("ALL", "ALL", vals[0]))
        else:
            print(
                PATTERN_FORMATTER.format(
                    ", ".join([task.name for task in tasks]), pattern, *vals, sum(vals)
                )
            )


def show_detailed_config_counts(config_file, db, todo=False):
    tasks_n, tasks_to_match, tasks_to_not_match = parse_config_file(config_file)
    for j in tasks_n:
        vals = []
        for i in range(1, 7):
            vals.append(
                db.execute(
                    state_table_query_builder(
                        "COUNT(*)", state=True, process_type=True
                    ),
                    (i, j.value),
                ).fetchone()[0]
            )
        if todo:
            print(PATTERN_TODO_FORMATTER.format("ALL", j.str_value, vals[0]))
        else:
            print(PATTERN_FORMATTER.format("ALL", j.str_value, *vals, sum(vals)))
    for pattern, tasks in tasks_to_match:
        for j in tasks:
            vals = []
            for i in range(1, 7):
                vals.append(
                    db.execute(
                        state_table_query_builder(
                            "COUNT(*)",
                            state=True,
                            process_type=True,
                            run_name_type=MgmtDB.ComparisonOperator.LIKE,
                        ),
                        (pattern, i, j.value),
                    ).fetchone()[0]
                )
            if todo:
                print(PATTERN_TODO_FORMATTER.format(pattern, j.str_value, vals[0]))
            else:
                print(PATTERN_FORMATTER.format(pattern, j.str_value, *vals, sum(vals)))
    for pattern, tasks in tasks_to_not_match:
        for j in tasks:
            vals = []
            for i in range(1, 7):
                vals.append(
                    db.execute(
                        state_table_query_builder(
                            "COUNT(*)",
                            state=True,
                            process_type=True,
                            run_name_type=MgmtDB.ComparisonOperator.NOTLIKE,
                        ),
                        (pattern, i, j.value),
                    ).fetchone()[0]
                )
            if todo:
                print(PATTERN_TODO_FORMATTER.format(pattern, j.str_value, vals[0]))
            else:
                print(PATTERN_FORMATTER.format(pattern, j.str_value, *vals, sum(vals)))


def show_all_error_entries(db, run_name, max_retries=False):
    extra_query = ""
    if max_retries:
        extra_query = RETRY_MAX_FILTER
    db.execute(
        """SELECT s.run_name, pe.proc_type, se.state, s.job_id, datetime(last_modified,'unixepoch') lm_time, e.error
            FROM state s
            JOIN status_enum se ON s.status = se.id
            JOIN proc_type_enum pe ON s.proc_type = pe.id
            JOIN error e ON s.id = e.task_id
            WHERE UPPER(s.run_name) LIKE UPPER(?) 
            """
        + extra_query
        + """
            ORDER BY s.run_name, se.id
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


def show_state_counts(db, todo=False):
    vals = []
    for i in range(1, 7):
        vals.append(
            db.execute(
                state_table_query_builder("COUNT(*)", state=True), (i,)
            ).fetchone()[0]
        )
    if todo:
        print(PATTERN_TODO_FORMATTER.format("All", "All", vals[0]))
    else:
        print(PATTERN_FORMATTER.format("All", "All", *vals, sum(vals)))


def print_mode_help():
    print(
        """--Mode Selection Help--
          Error:
              Prints out all tasks that have errored along with an appropriate error message
              Cannot be used with count / detailed_count / todo
          Count:
              Prints out the count of tasks in each state
              Cannot be used with Error
          Detailed Count:
              Prints out the count of tasks in each state for each process type
              Cannot be used with Error
          Retry Max:
              Filters the tasks that have reached the retry count
              Cannot be used with count / todo
          Todo:
              Prints out all the non-complete tasks - only shows the latest status
              Cannot be used with retry max
             """
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "run_folder", type=str, help="path to simulation root directory"
    )
    parser.add_argument(
        "run_name", type=str, nargs="?", default="%", help="name of run to be queried"
    )
    parser.add_argument(
        "--mode",
        "-m",
        choices=["error", "count", "todo", "retry_max", "detailed_count"],
        nargs="+",
        help="changes the mode of the program to provide different information. --mode-help for more information",
        default=[],
    )

    parser.add_argument(
        "--config",
        type=str,
        default=None,
        help="The cybershake config file defining which tasks are being run, and should be looked at ",
    )

    parser.add_argument(
        "--mode-help",
        action="store_true",
        help="prints details about what each mode does",
    )

    args = parser.parse_args()
    if args.mode_help:
        print_mode_help()
        exit()
    f = args.run_folder
    run_name = args.run_name
    mode = args.mode
    db = MgmtDB.connect_db(f)

    query_mode = QueryModes()
    if "error" in mode:
        query_mode.error = True
    if "todo" in mode:
        query_mode.todo = True
    if "count" in mode:
        query_mode.count = True
    if "detailed_count" in mode:
        query_mode.count = True
        query_mode.detailed_count = True
    if "retry_max" in mode:
        query_mode.retry_max = True

    print_run_status(db, run_name, query_mode, args.config)


if __name__ == "__main__":
    main()
