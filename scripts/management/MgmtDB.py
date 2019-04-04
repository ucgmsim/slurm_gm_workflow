import os
import sqlite3 as sql

from typing import List, Union
from collections import namedtuple

import qcore.constants as const
from scripts.management.db_helper import connect_db_ctx

Process = const.ProcessType

SlurmTask = namedtuple(
    "SlurmTask", ["run_name", "proc_type", "status", "job_id", "retries"]
)


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

    def update_entries_live(self, entries: List[SlurmTask]):
        """Updates the specified entries in the db. Leaves the connection open,
        so this should only be used when continuously updating entries.
        """
        try:
            if self._conn is None:
                self._conn = sql.connect(self._db_file)

            cur = self._conn.cursor()

            for entry in entries:
                self._update_entry(cur, entry)
        except sql.Error as ex:
            self._conn.rollback()
            print(
                "Failed to update entry {}, due to the exception: \n{}".format(
                    entry, ex
                )
            )
        else:
            self._conn.commit()
        finally:
            cur.close()

        return True

    def get_submitted_tasks(self):
        """Gets all in progress tasks i.e. (running or queued)"""
        with connect_db_ctx(self._db_file) as cur:
            result = cur.execute(
                "SELECT run_name, proc_type, status, job_id, retries "
                "FROM state WHERE status IN (2, 3)"
            ).fetchall()

        return [SlurmTask(*entry) for entry in result]

    def get_runnable_tasks(self, n_runs, retry_max):
        """Gets all runnable tasks based on their status and their associated
        dependencies (i.e. other tasks have to be finished first)"""
        do_verification = False
        verification_tasks = [
            Process.rrup.value,
            Process.Empirical.value,
            Process.Verification.value,
        ]

        with connect_db_ctx(self._db_file) as cur:
            db_tasks = cur.execute(
                """SELECT proc_type, run_name, status_enum.state 
                          FROM status_enum, state 
                          WHERE state.status = status_enum.id
                           AND ((status_enum.state = 'created' 
                                 AND state.retries < ?)
                            OR status_enum.state = 'completed')""",
                (retry_max,),
            ).fetchall()

        tasks_to_run = []
        for task in db_tasks:
            status = task[2]
            if status == "created" and self._check_dependancy_met(task, db_tasks):
                if task[0] not in verification_tasks or do_verification:
                    tasks_to_run.append(task)
            if len(tasks_to_run) >= n_runs:
                break

        return tasks_to_run

    @staticmethod
    def is_task_complete(task, task_list):
        process, run_name, status = task
        for check_task in task_list:
            if (
                check_task[0] == process
                and check_task[1] == run_name
                and check_task[2] == status
            ):
                return True
        return False

    def _check_dependancy_met(self, task, task_list):
        """Checks if all dependencies for the specified are met"""
        process, run_name, status = task
        process = Process(process)
        if process in (Process.EMOD3D, Process.HF, Process.rrup):
            return True

        # If the process has completed the one linearly before it
        if process in (
            Process.merge_ts,
            Process.winbin_aio,
            Process.IM_calculation,
            Process.Empirical,
        ):
            dependant_task = list(task)
            dependant_task[0] = process.value - 1
            dependant_task[2] = "completed"
            return self.is_task_complete(dependant_task, task_list)

        if process is Process.BB:
            LF_task = list(task)
            LF_task[0] = Process.EMOD3D.value
            LF_task[2] = "completed"
            HF_task = list(task)
            HF_task[0] = Process.HF.value
            HF_task[2] = "completed"
            return self.is_task_complete(
                LF_task, task_list
            ) and self.is_task_complete(HF_task, task_list)

        if process is Process.clean_up:
            IM_task = list(task)
            IM_task[0] = Process.IM_calculation.value
            IM_task[2] = "completed"
            merge_ts_task = list(task)
            merge_ts_task[0] = Process.merge_ts.value
            merge_ts_task[2] = "completed"
            return self.is_task_complete(
                IM_task, task_list
            ) and self.is_task_complete(merge_ts_task, task_list)

        return False

    def _update_entry(self, cur: sql.Cursor, entry: SlurmTask):
        """Updates all fields that have a value for the specific entry"""
        for field, value in zip(
            [
                (self.col_status, self.col_job_id, self.col_retries),
                (entry.status, entry.job_id, entry.retries),
            ]
        ):
            if value is not None:
                cur.execute(
                    "UPDATE state SET ? = ? WHERE run_name = ? AND proc_type = ?",
                    (field, value, entry.run_name, entry.proc_type),
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
