import os
import sqlite3 as sql
from logging import Logger

from typing import List, Union
from collections import namedtuple

import qcore.constants as const
from scripts.management.db_helper import connect_db_ctx
from shared_workflow import workflow_logger

Process = const.ProcessType

SlurmTask = namedtuple(
    "SlurmTask", ["run_name", "proc_type", "status", "job_id", "retries", "error"]
)
# Make error an optional value, once we are using Python 3.7 then this can be made nicer..
SlurmTask.__new__.__defaults__ = (None,)


class MgmtDB:

    # State Column names
    col_run_name = "run_name"
    col_proc_type = "proc_type"
    col_status = "status"
    col_job_id = "job_id"
    col_retries = "retries"

    def __init__(self, db_file: str):
        self._db_file = db_file

        # This should only be used when doing actions with the intention of
        # leaving the connection open, otherwise use connect_db_ctx with a "with"
        # statement.
        self._conn = None

    @property
    def db_file(self):
        return self._db_file

    def update_entries_live(self, entries: List[SlurmTask], retry_max: int, logger: Logger):
        """Updates the specified entries in the db. Leaves the connection open,
        so this should only be used when continuously updating entries.
        """
        try:
            if self._conn is None:
                logger.info("Aquiring db connection.")
                self._conn = sql.connect(self._db_file)
            cur = self._conn.cursor()

            for entry in entries:
                process = entry.proc_type
                realisation_name = entry.run_name
                if entry.status == const.Status.created.value:
                    # Something has attempted to set a task to created
                    # Make a new task with created status and move to the next task
                    self._insert_task(cur, realisation_name, process)
                    continue

                self._update_entry(cur, entry)

                if entry.status == const.Status.failed.value:
                    # The task was failed. If there have been few enough other attempts at the task make another one
                    with connect_db_ctx(self._db_file) as cur:
                        cur_retries = cur.execute(
                            "SELECT retries from state "
                            "WHERE run_name = ? AND proc_type = ?",
                            (realisation_name, process),
                        ).fetchone()[0]
                        if cur_retries < retry_max:
                            self._insert_task(cur, realisation_name, process)
        except sql.Error as ex:
            self._conn.rollback()
            print(
                "Failed to update entry {}, due to the exception: \n{}".format(
                    entry, ex
                )
            )
            return False
        else:
            self._conn.commit()
        finally:
            cur.close()

        return True

    def close_conn(self):
        """Close the db connection. Note, this ONLY has to be done if
        update_entries_live was used. In all other scenarios the connection is
        closed by default."""
        if self._conn is not None:
            self._conn.close()

    def get_submitted_tasks(self, allowed_tasks=tuple(const.ProcessType)):
        """Gets all in progress tasks i.e. (running or queued)"""
        allowed_tasks = [str(task.value) for task in allowed_tasks]
        with connect_db_ctx(self._db_file) as cur:
            result = cur.execute(
                "SELECT run_name, proc_type, status, job_id, retries "
                "FROM state WHERE status IN (2, 3) AND proc_type IN (?{})".format(",?"*(len(allowed_tasks)-1)),
                allowed_tasks
            ).fetchall()

        return [SlurmTask(*entry) for entry in result]

    def get_runnable_tasks(self, allowed_rels, allowed_tasks=None, logger=workflow_logger.get_basic_logger()):
        """Gets all runnable tasks based on their status and their associated
        dependencies (i.e. other tasks have to be finished first)

        Returns a list of tuples (proc_type, run_name, state_str)
        """

        if allowed_tasks is None:
            allowed_tasks = list(const.ProcessType)
        allowed_tasks = [str(task.value) for task in allowed_tasks]

        with connect_db_ctx(self._db_file) as cur:
            db_tasks = cur.execute(
                """SELECT proc_type, run_name 
                          FROM status_enum, state 
                          WHERE state.status = status_enum.id
                           AND proc_type IN (?{})
                           AND run_name LIKE (?)
                           AND status_enum.state = 'created'""".format(
                    ",?" * (len(allowed_tasks) - 1)
                ),
                (*allowed_tasks, allowed_rels),
            ).fetchall()

        return [task for task in db_tasks if self._check_dependancy_met(task, logger)]

    def is_task_complete(self, task):
        process, run_name, status = task
        with connect_db_ctx(self._db_file) as cur:
            state = cur.execute(
                """SELECT state.status 
                          FROM state 
                          WHERE run_name = (?)
                           AND proc_type = (?)""",
                (run_name, process),
            ).fetchone()[0]
        return state == const.Status.completed.value

    def _check_dependancy_met(self, task, logger=workflow_logger.get_basic_logger()):
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
                (run_name, ),
            ).fetchall()
        logger.debug("Considering task {} for realisation {}. Completed tasks as follows: {}".format(process, run_name, completed_tasks))
        remaining_deps = process.get_remaining_dependencies([const.ProcessType(x[0]) for x in completed_tasks])
        logger.debug("{} has remaining deps: {}".format(task, remaining_deps))
        return len(remaining_deps) == 0

    def _update_entry(self, cur: sql.Cursor, entry: SlurmTask):
        """Updates all fields that have a value for the specific entry"""
        for field, value in zip(
            (self.col_status, self.col_job_id, self.col_retries),
            (entry.status, entry.job_id, entry.retries),
        ):
            if value is not None:
                cur.execute(
                    "UPDATE state SET {} = ?, last_modified = strftime('%s','now') "
                    "WHERE run_name = ? AND proc_type = ?".format(field),
                    (value, entry.run_name, entry.proc_type),
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
                        self._insert_task(cur, run_name, proc[0])

    def insert(self, run_name: str, proc_type: int):
        """Inserts a task into the mgmt db"""
        with connect_db_ctx(self._db_file) as cur:
            self._insert_task(cur, run_name, proc_type)

    def _insert_task(self, cur: sql.Cursor, run_name: str, proc_type: int):
        cur.execute(
            """INSERT OR IGNORE INTO `state`(run_name, proc_type, status, 
            last_modified, retries) VALUES(?, ?, 1, strftime('%s','now'), 0)""",
            (run_name, proc_type),
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
