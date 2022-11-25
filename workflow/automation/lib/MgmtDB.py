import datetime
from collections import namedtuple
from contextlib import contextmanager
from logging import Logger
import os
import sqlite3 as sql
from typing import List, Union
from dataclasses import dataclass

import qcore.constants as const
from workflow.automation.lib.constants import ChCountType
from qcore.qclogging import get_basic_logger

Process = const.ProcessType


@dataclass
class SchedulerTask:
    run_name: str
    proc_type: int
    status: int
    job_id: int
    error: str = None
    queued_time: int = None
    start_time: int = None
    end_time: int = None
    nodes: int = None
    cores: int = None
    memory: int = None
    wct: int = None


def connect_db(path):
    db_location = os.path.abspath(os.path.join(path, "slurm_mgmt.db"))
    conn = sql.connect(db_location)
    db = conn.cursor()
    db.execute("PRAGMA synchronous = EXTRA")
    # db.execute("PRAGMA synchronous = OFF")
    db.execute("PRAGMA integrity_check")

    return db


@contextmanager
def connect_db_ctx(db_file, verbose=False):
    """Returns a db cursor. Use with a context (i.e. with statement)

    A commit is run at the end of the context.
    """
    conn = sql.connect(db_file)
    if verbose:
        conn.set_trace_callback(print)

    try:
        yield conn.cursor()
    except Exception:
        conn.rollback()
        raise
    else:
        conn.commit()
    finally:
        conn.close()


def enum_to_list(enum):
    return [x.name for x in enum]


class MgmtDB:
    # State Column names
    col_run_name = "run_name"
    col_proc_type = "proc_type"
    col_status = "status"
    col_job_id = "job_id"
    col_queued_time = "queued_time"
    col_start_time = "start_time"
    col_end_time = "end_time"
    col_nodes = "nodes"
    col_cores = "cores"
    col_memory = "memory"
    col_wct = "WCT"

    def __init__(self, db_file: str):
        self._db_file = db_file

        # This should only be used when doing actions with the intention of
        # leaving the connection open, otherwise use connect_db_ctx with a "with"
        # statement.
        self._conn = None

    @property
    def db_file(self):
        return self._db_file

    def get_retries(self, process, realisation_name, get_WCT=False):
        get_WCT_symbol = "=" if get_WCT else "!="
        with connect_db_ctx(self._db_file) as cur:
            return cur.execute(
                "SELECT COUNT(*) from state "
                f"WHERE run_name = ? AND proc_type = ? and status {get_WCT_symbol} ?",
                (realisation_name, process, const.Status.killed_WCT.value),
            ).fetchone()[0]

    def update_entries_live(
        self,
        entries: List[SchedulerTask],
        retry_max: int,
        logger: Logger = get_basic_logger(),
    ):
        """Updates the specified entries in the db. Leaves the connection open,
        so this should only be used when continuously updating entries.
        """
        try:
            if self._conn is None:
                logger.info("Acquiring db connection.")
                self._conn = sql.connect(self._db_file)
            logger.debug("Getting db cursor")

            cur = self._conn.cursor()
            cur.execute("BEGIN")
            for entry in entries:
                process = entry.proc_type
                realisation_name = entry.run_name

                logger.debug(
                    "The status of process {} for realisation {} is being set to {}. It has slurm id {}".format(
                        entry.proc_type, entry.run_name, entry.status, entry.job_id
                    )
                )

                if entry.status == const.Status.queued.value:
                    # Add queued time metadata to the job log when the job has been queued
                    logger.debug("Logging queued task to the db")
                    self.insert_job_log(
                        cur,
                        entry.job_id,
                        entry.queued_time,
                    )

                if entry.status == const.Status.running.value:
                    # Add general metadata to the job log when the task has started running
                    logger.debug("Logging running task to the db")
                    self.update_job_log(
                        cur,
                        entry.job_id,
                        entry.start_time,
                        entry.nodes,
                        entry.cores,
                        entry.memory,
                        entry.wct,
                    )

                if entry.status == const.Status.created.value:
                    # Something has attempted to set a task to created
                    # Make a new task with created status and move to the next task
                    logger.debug("Adding new task to the db")
                    # Check that there isn't already a task with the same realisation name
                    if self._does_task_exists(cur, realisation_name, process):
                        logger.debug(
                            "task is already in progress - does not need to be readded"
                        )
                        continue
                    self._insert_task(cur, realisation_name, process)
                    logger.debug("New task added to the db, continuing to next process")
                    continue
                logger.debug("Updating task in the db")
                self._update_entry(cur, entry, logger=logger)
                logger.debug("Task successfully updated")

                if (
                    entry.status == const.Status.failed.value
                    or entry.status == const.Status.killed_WCT.value
                    or entry.status == const.Status.completed.value
                ):
                    # Update the job duration log if task has failed, killed by WCT or completed
                    logger.debug("Logging end time for the task to the db")
                    self.update_end_job_log(
                        cur,
                        entry.job_id,
                        int(datetime.datetime.now().timestamp())
                        if entry.end_time == ""
                        else entry.end_time,
                    )

                if (
                    entry.status == const.Status.failed.value
                    and self.get_retries(process, realisation_name, get_WCT=False)
                    < retry_max
                ):
                    # The task was failed. If there have been few enough other attempts at the task make another one
                    logger.debug(
                        "Task failed but is able to be retried. Adding new task to the db"
                    )
                    self._insert_task(cur, realisation_name, process)
                    logger.debug("New task added to the db")

                if (
                    entry.status == const.Status.killed_WCT.value
                    and self.get_retries(process, realisation_name, get_WCT=True)
                    < retry_max
                ):
                    # The task was killed_WCT. If there have been few enough other attempts at the task make another one
                    logger.debug(
                        "Task hit WCT but is able to be retried. Adding new task to the db"
                    )
                    self._insert_task(cur, realisation_name, process)
                    logger.debug("New task added to the db")

                if entry.status == const.Status.failed.value:
                    tasks = MgmtDB.find_dependant_task(cur, entry)
                    i = 0
                    while i < len(tasks):
                        task = tasks[i]
                        # fails dependant task
                        self._update_entry(cur, task, logger=logger)
                        logger.debug(
                            f"Cascading failure for {entry.run_name} - {task.proc_type}"
                        )
                        tasks.extend(MgmtDB.find_dependant_task(cur, task))
                        i += 1

        except sql.Error as ex:
            self._conn.rollback()
            logger.critical(
                "Failed to update entry {}, due to the exception: \n{}".format(
                    entry, ex
                )
            )
            return False
        else:
            logger.debug("Committing changes to db")
            self._conn.commit()
        finally:
            logger.debug("Closing db cursor")
            cur.close()

        return True

    def get_core_hour_states(self, rel_name: str, ch_count_type: ChCountType):
        with connect_db_ctx(self._db_file) as cur:
            if ch_count_type == ChCountType.Needed:
                states = []
                # Selects only tasks after the last failed attempt
                proc_types = cur.execute(
                    "SELECT DISTINCT proc_type from state WHERE run_name=? AND status!=?",
                    (rel_name, const.Status.created.value),
                ).fetchall()
                for proc_type in proc_types:
                    proc_type = proc_type[0]
                    failed_task_modified = cur.execute(
                        "SELECT last_modified from state WHERE run_name=? AND status=? AND proc_type=?",
                        (rel_name, const.Status.failed.value, proc_type),
                    ).fetchall()
                    if len(failed_task_modified) > 0:
                        # Only select tasks for this proc_type that were modified after the last failed task
                        failed_task_modified_time = failed_task_modified[-1][0]
                        states.extend(
                            cur.execute(
                                "SELECT * from state WHERE run_name=? AND "
                                "(status=? OR status=?) AND proc_type=? AND last_modified>?",
                                (
                                    rel_name,
                                    const.Status.completed.value,
                                    const.Status.killed_WCT.value,
                                    proc_type,
                                    failed_task_modified_time,
                                ),
                            ).fetchall()
                        )
                    else:
                        # There were no failed tasks for this proc_type
                        states.extend(
                            cur.execute(
                                "SELECT * from state WHERE run_name=? AND (status=? OR status=?) AND proc_type=?",
                                (
                                    rel_name,
                                    const.Status.completed.value,
                                    const.Status.killed_WCT.value,
                                    proc_type,
                                ),
                            ).fetchall()
                        )
            else:
                states = cur.execute(
                    "SELECT * from state WHERE run_name=? AND (status=? OR status=?)",
                    (
                        rel_name,
                        const.Status.completed.value,
                        const.Status.killed_WCT.value,
                    ),
                ).fetchall()
        return states

    def get_job_duration_info(self, job_id: int):
        with connect_db_ctx(self._db_file) as cur:
            return cur.execute(
                "SELECT * from job_duration_log WHERE job_id=?",
                (job_id,),
            ).fetchone()

    def get_rel_names(self):
        with connect_db_ctx(self._db_file) as cur:
            return cur.execute("SELECT DISTINCT run_name from state").fetchall()

    @staticmethod
    def find_dependant_task(cur, entry):
        tasks = []
        for process in const.ProcessType:
            for dependency in process.dependencies:
                if isinstance(dependency, tuple):
                    dependency = dependency[0]
                if entry.proc_type == dependency:
                    job_id = cur.execute(
                        "SELECT `job_id` FROM `state` WHERE proc_type = ? and status = ? and run_name = ?",
                        (process.value, const.Status.completed.value, entry.run_name),
                    ).fetchone()

                    if job_id is not None:
                        dependant_entry = SchedulerTask(
                            entry.run_name,
                            process.value,
                            const.Status.failed.value,
                            job_id[0],
                        )
                        tasks.append(dependant_entry)
        return tasks

    def add_retries(self, n_max_retries: int):
        """Checks the database for failed tasks with less failures than the given n_max_retries.
        If any are found then the tasks are checked for any entries that are created, queued, running or completed.
        If any are found then nothing happens, if none are found then another created entry is added to the db.
        n_max_retries: The maximum number of retries a task can have"""
        with connect_db_ctx(self._db_file) as cur:
            errored = cur.execute(
                "SELECT run_name, proc_type "
                "FROM state, status_enum "
                "WHERE state.status = status_enum.id "
                "AND (status_enum.state  = 'failed' "
                "OR status_enum.state = 'killed_WCT')"
            ).fetchall()

        failure_count = {}
        for run_name, proc_type in errored:
            key = "{}__{}".format(run_name, proc_type)
            if key not in failure_count.keys():
                failure_count.update({key: 0})
            failure_count[key] += 1

        with connect_db_ctx(self._db_file) as cur:
            for key, fail_count in failure_count.items():
                if fail_count >= n_max_retries:
                    continue
                run_name, proc_type = key.split("__")
                # Gets the number of entries for the task with state in [created, queued, running or completed]
                # Where completed has enum index 5, and the other 4 less than this
                # If any are found then don't add another entry
                not_failed_count = cur.execute(
                    "SELECT COUNT(*) "
                    "FROM state "
                    "WHERE run_name = (?)"
                    "AND proc_type = (?)"
                    "AND status <= (SELECT id FROM status_enum WHERE state = 'completed') ",
                    (run_name, proc_type),
                ).fetchone()[0]
                if not_failed_count == 0:
                    self._insert_task(cur, run_name, proc_type)

    def close_conn(self):
        """Close the db connection. Note, this ONLY has to be done if
        update_entries_live was used. In all other scenarios the connection is
        closed by default."""
        if self._conn is not None:
            self._conn.close()

    def get_submitted_tasks(self, allowed_tasks=tuple(const.ProcessType)):
        """Gets all in progress tasks i.e. (running or queued)"""
        return self.command_builder(
            allowed_tasks=allowed_tasks,
            allowed_states=[const.Status.queued, const.Status.running],
        )

    def get_runnable_tasks(
        self,
        allowed_rels,
        task_limit,
        update_files,
        allowed_tasks=None,
        logger=get_basic_logger(),
    ):
        """Gets all runnable tasks based on their status and their associated
        dependencies (i.e. other tasks have to be finished first)

        Returns a list of tuples (proc_type, run_name, state_str)
        """
        if allowed_tasks is None:
            allowed_tasks = list(const.ProcessType)
        allowed_tasks = [str(task.value) for task in allowed_tasks]

        if len(allowed_tasks) == 0:
            return []

        runnable_tasks = []
        offset = 0

        # "{}__{}" is intended to be the template for a unique string for every realisation and process type pair
        # Used to compare with database entries to prevent running a task that has already been submitted, but not
        # recorded
        tasks_waiting_for_updates = [
            "{}__{}".format(*(entry.split(".")[1:3])) for entry in update_files
        ]

        with connect_db_ctx(self._db_file) as cur:
            entries = cur.execute(
                """SELECT COUNT(*) 
                          FROM status_enum, state 
                          WHERE state.status = status_enum.id
                           AND proc_type IN (?{})
                           AND run_name LIKE (?)
                           AND status_enum.state = 'created'""".format(
                    ",?" * (len(allowed_tasks) - 1)
                ),
                (*allowed_tasks, allowed_rels),
            ).fetchone()[0]

            while len(runnable_tasks) < task_limit and offset < entries:
                db_tasks = cur.execute(
                    """SELECT proc_type, run_name 
                              FROM status_enum, state 
                              WHERE state.status = status_enum.id
                               AND proc_type IN (?{})
                               AND run_name LIKE (?)
                                   AND status_enum.state = 'created'
                                   LIMIT 100 OFFSET ?""".format(
                        ",?" * (len(allowed_tasks) - 1)
                    ),
                    (*allowed_tasks, allowed_rels, offset),
                ).fetchall()
                runnable_tasks.extend(
                    [
                        (*task, self.get_retries(*task, get_WCT=True))
                        for task in db_tasks
                        if self._check_dependancy_met(task, logger)
                        and "{}__{}".format(*task) not in tasks_waiting_for_updates
                    ]
                )
                offset += 100

        return runnable_tasks

    def num_task_complete(self, task, like=False):
        process, run_name = task
        op = "LIKE" if like else "="

        query = f"SELECT COUNT (*) FROM state WHERE run_name {op} ? AND proc_type = ? AND status = ?"
        with connect_db_ctx(self._db_file) as cur:
            completed_tasks = cur.execute(
                query, (run_name, process, const.Status.completed.value)
            ).fetchone()[0]
        return completed_tasks

    def is_task_complete(self, task):
        return self.num_task_complete(task) > 0

    def _check_dependancy_met(self, task, logger=get_basic_logger()):
        """Checks if all dependencies for the specified are met"""
        process, run_name = task
        process = Process(process)

        with connect_db_ctx(self._db_file) as cur:
            completed_tasks = cur.execute(
                """SELECT proc_type 
                          FROM status_enum, state 
                          WHERE state.status = status_enum.id
                           AND run_name = (?)
                           AND status_enum.state = 'completed'""",
                (run_name,),
            ).fetchall()
        logger.debug(
            "Considering task {} for realisation {}. Completed tasks as follows: {}".format(
                process, run_name, completed_tasks
            )
        )
        remaining_deps = process.get_remaining_dependencies(
            [const.ProcessType(x[0]) for x in completed_tasks]
        )
        logger.debug("{} has remaining deps: {}".format(task, remaining_deps))
        return len(remaining_deps) == 0

    def _update_entry(
        self, cur: sql.Cursor, entry: SchedulerTask, logger: Logger = get_basic_logger()
    ):
        """Updates all fields that have a value for the specific entry"""
        if entry.status == const.Status.queued.value:
            logger.debug(
                "Got entry {} with status queued. Setting status and job id in the db".format(
                    entry
                )
            )
            cur.execute(
                "UPDATE state SET {} = ?, {} = ?, last_modified = strftime('%s','now') "
                "WHERE run_name = ? AND proc_type = ? and status < ?".format(
                    self.col_job_id, self.col_status
                ),
                (
                    entry.job_id,
                    entry.status,
                    entry.run_name,
                    entry.proc_type,
                    entry.status,
                ),
            )
        elif entry.job_id is not None:
            cur.execute(
                "UPDATE state SET status = ?, last_modified = strftime('%s','now') "
                "WHERE run_name = ? AND proc_type = ? and status < ? and job_id = ?",
                (
                    entry.status,
                    entry.run_name,
                    entry.proc_type,
                    entry.status,
                    entry.job_id,
                ),
            )
        else:
            logger.warning(
                "Received entry {}, status is more than created but the job_id is not set.".format(
                    entry
                )
            )
            cur.execute(
                "UPDATE state SET status = ?, last_modified = strftime('%s','now') "
                "WHERE run_name = ? AND proc_type = ? and status < ?",
                (entry.status, entry.run_name, entry.proc_type, entry.status),
            )
        if cur.rowcount > 1:
            logger.warning(
                "Last database update caused {} entries to be updated".format(
                    cur.rowcount
                )
            )
        if entry.error is not None:
            cur.execute(
                """INSERT INTO error (task_id, error)
                  VALUES (
                  (SELECT id from state WHERE proc_type = ? AND run_name = ?), ?)""",
                (entry.proc_type, entry.run_name, entry.error),
            )

    def populate(self, realisations, srf_files: Union[List[str], str] = []):
        """Initial population of the database with all realisations"""
        # for manual install, only one srf will be passed to srf_files as a string
        if isinstance(srf_files, str):
            srf_files = [srf_files]

        realisations.extend(
            [os.path.splitext(os.path.basename(srf))[0] for srf in srf_files]
        )

        if len(realisations) == 0:
            print("No realisations found - no entries inserted into db")
        else:
            with connect_db_ctx(self._db_file) as cur:
                procs_to_be_done = cur.execute(
                    """select * from proc_type_enum"""
                ).fetchall()

                for run_name in realisations:
                    for proc in procs_to_be_done:
                        if not self._does_task_exists(cur, run_name, proc[0]):
                            self._insert_task(cur, run_name, proc[0])

    def insert(self, run_name: str, proc_type: int):
        """Inserts a task into the mgmt db"""
        with connect_db_ctx(self._db_file) as cur:
            self._insert_task(cur, run_name, proc_type)

    @staticmethod
    def _insert_task(cur: sql.Cursor, run_name: str, proc_type: int):
        cur.execute(
            """INSERT OR IGNORE INTO `state`(run_name, proc_type, status, 
            last_modified) VALUES(?, ?, 1, strftime('%s','now'))""",
            (run_name, proc_type),
        )

    @staticmethod
    def _does_task_exists(cur: sql.Cursor, run_name: str, proc_type: int):
        """Checks if there is a non-failed task with the same name"""
        count = cur.execute(
            "SELECT COUNT(*) from `state`, `status_enum` "
            "WHERE `run_name` = ? "
            "AND `proc_type` = ? "
            "AND state.status = status_enum.id "
            "AND status_enum.state <> 'failed'",
            (run_name, proc_type),
        ).fetchone()[0]
        return count > 0

    @staticmethod
    def insert_job_log(
        cur: sql.Cursor,
        job_id: int,
        queued_time: int,
    ):
        cur.execute(
            """INSERT OR IGNORE INTO `job_duration_log`(job_id, queued_time)
             VALUES(?, ?)""",
            (job_id, queued_time),
        )

    @staticmethod
    def update_job_log(
        cur: sql.Cursor,
        job_id: int,
        start_time: int = None,
        nodes: int = None,
        cores: int = None,
        memory: int = None,
        wct: int = None,
    ):
        cur.execute(
            "UPDATE job_duration_log SET start_time = ?, nodes = ?, cores = ?, memory = ?, WCT = ? WHERE job_id = ?",
            (start_time, nodes, cores, memory, wct, job_id),
        )

    @staticmethod
    def update_end_job_log(
        cur: sql.Cursor,
        job_id: int,
        end_time: int = None,
    ):
        cur.execute(
            "UPDATE job_duration_log SET end_time = ? WHERE job_id = ?",
            (end_time, job_id),
        )

    @classmethod
    def init_db(cls, db_file: str, init_script: str):
        with connect_db_ctx(db_file) as cur:
            with open(init_script, "r") as f:
                cur.executescript(f.read())

        return cls(db_file)

    def __del__(self):
        if self._conn is not None:
            self._conn.close()

    def command_builder(
        self,
        allowed_tasks: List[const.ProcessType] = None,
        blocked_tasks: List[const.ProcessType] = None,
        allowed_states: List[const.Status] = None,
        blocked_states: List[const.Status] = None,
        allowed_ids: List[int] = None,
        blocked_ids: List[int] = None,
    ):
        """
        Allows for retrieving custom collections of database entries
        Allowed and blocked are mutually exclusive with allowed being used first. No error is raised if both are present
        If any list is empty this is treated as if it were None
        :param allowed_tasks, blocked_tasks: a list of process types to either block or exclusively allow
        :param allowed_states, blocked_states: a list of states to either block or exclusively allow
        :param allowed_ids, blocked_ids: a list of job ids to either block or exclusively allow
        :return: A list of Entry objects
        """

        base_command = (
            "SELECT run_name, proc_type, state.status, job_id "
            "FROM state, status_enum "
            "WHERE state.status = status_enum.id "
        )
        arguments = []

        if allowed_tasks is not None and len(allowed_tasks) > 0:
            allowed_tasks = [str(task.value) for task in allowed_tasks]
            base_command += " AND proc_type IN ({})".format(
                ",".join("?" * len(allowed_tasks))
            )
            arguments.extend(allowed_tasks)
        elif blocked_tasks is not None and len(blocked_tasks) > 0:
            blocked_tasks = [str(task.value) for task in blocked_tasks]
            base_command += " AND proc_type NOT IN ({})".format(
                ",".join("?" * len(blocked_tasks))
            )
            arguments.extend(blocked_tasks)

        if allowed_states is not None and len(allowed_states) > 0:
            allowed_states = [str(state.str_value) for state in allowed_states]
            base_command += " AND status_enum.state IN ({})".format(
                ",".join("?" * len(allowed_states))
            )
            arguments.extend(allowed_states)
        elif blocked_states is not None and len(blocked_states) > 0:
            blocked_states = [str(state.str_value) for state in blocked_states]
            base_command += " AND status_enum.state NOT IN ({})".format(
                ",".join("?" * len(blocked_states))
            )
            arguments.extend(blocked_states)

        if allowed_ids is not None and len(allowed_ids) > 0:
            allowed_ids = [str(state) for state in allowed_ids]
            base_command += " AND job_id IN ({})".format(
                ",".join("?" * len(allowed_ids))
            )
            arguments.extend(allowed_ids)
        elif blocked_ids is not None and len(blocked_ids) > 0:
            blocked_ids = [str(state) for state in blocked_ids]
            base_command += " AND job_id NOT IN ({})".format(
                ",".join("?" * len(blocked_ids))
            )
            arguments.extend(blocked_ids)

        with connect_db_ctx(self._db_file) as cur:
            result = cur.execute(base_command, arguments).fetchall()

        return [SchedulerTask(*entry) for entry in result]
