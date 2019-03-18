import os
import sqlite3 as sql
from collections import namedtuple
from enum import Enum

from typing import Iterable, Union
from datetime import datetime, date
from contextlib import contextmanager


import qcore.constants as const

SQueueEntry = namedtuple(
    "SQueue",
    [
        "job_id",
        "username",
        "status",
        "name",
        "run_time",
        "est_run_time",
        "nodes",
        "cores",
    ],
)

StatusEntry = namedtuple(
    "StatusEntry", ["id", "name", "int_value_1", "int_value_2", "update_time"]
)


QuotaEntry = namedtuple(
    "QuotaEntry", ["file_system", "used_space", "available_inodes", "used_inodes", "day"]
)


class HPCProperty(Enum):
    node_capacity = 1, "node_capacity"

    def __new__(cls, value, str_value):
        obj = object.__new__(cls)
        obj._value_ = value
        obj.str_value = str_value
        return obj


class DashboardDB:

    date_format = "%Y-%m-%d"

    def __init__(self, db_file: str):
        """Opens an existing dashboard database file."""

        if not os.path.isfile(db_file):
            raise FileNotFoundError(
                "Specified database {} does not exist. Use static create_db "
                "function to create a new dashboard db file.".format(db_file)
            )

        self.db_file = db_file

    @staticmethod
    def get_daily_t_name(hpc: const.HPC):
        return "{}_DAILY".format(hpc.value.upper())

    @staticmethod
    def get_squeue_t_name(hpc: const.HPC):
        return "{}_SQUEUE".format(hpc.value.upper())

    @staticmethod
    def get_quota_t_name(hpc: const.HPC):
        return "{}_QUOTA".format(hpc.value.upper())

    def update_daily_chours_usage(
        self,
        total_core_usage: float,
        hpc: const.HPC,
        day: Union[date, str] = date.today(),
    ):
        """Updates the daily core hours usage.

        The core hour usage for a day is calculated as
        last saved (for current day) TOTAL_CORE_USAGE - current total_core_usage.
        """
        # Do nothing if total_core_usage is None
        if total_core_usage is None:
            return

        table = self.get_daily_t_name(hpc)
        day = day.strftime(self.date_format) if type(day) is date else day
        with self.get_cursor(self.db_file) as cursor:
            row = cursor.execute(
                "SELECT CORE_HOURS_USED, TOTAL_CORE_HOURS FROM {} WHERE DAY == ?;".format(
                    table
                ),
                (day,),
            ).fetchone()

            # New day
            update_time = datetime.now()
            if row is None:
                cursor.execute(
                    "INSERT INTO {} VALUES(?, ?, ?, ?)".format(table),
                    (day, 0, total_core_usage, update_time),
                )
            else:
                chours_usage = total_core_usage - row[1] if row[1] is not None else 0
                if row[0] is not None:
                    chours_usage = row[0] + chours_usage

                cursor.execute(
                    "UPDATE {} SET CORE_HOURS_USED = ?, TOTAL_CORE_HOURS = ?, \
                    UPDATE_TIME = ? WHERE DAY == ?;".format(
                        table
                    ),
                    (chours_usage, total_core_usage, update_time, day),
                )

    def get_chours_usage(
        self, start_date: Union[date, str], end_date: Union[date, str], hpc: const.HPC
    ):
        """Gets the usage and total usage for the date range"""
        start_date = (
            start_date.strftime(self.date_format)
            if type(start_date) is date
            else start_date
        )
        end_date = (
            end_date.strftime(self.date_format) if type(end_date) is date else end_date
        )
        with self.get_cursor(self.db_file) as cursor:
            results = cursor.execute(
                "SELECT DAY, CORE_HOURS_USED, TOTAL_CORE_HOURS FROM {} "
                "WHERE DAY BETWEEN ? AND ?".format(self.get_daily_t_name(hpc)),
                (start_date, end_date),
            ).fetchall()

        return results

    def update_squeue(self, squeue_entries: Iterable[SQueueEntry], hpc: const.HPC):
        """Updates the squeue table with the latest queue status"""
        table = self.get_squeue_t_name(hpc)

        with self.get_cursor(self.db_file) as cursor:
            # Drop and re-create the table
            cursor.execute("DROP TABLE IF EXISTS {}".format(table))
            self._create_queue_table(cursor, hpc)

            update_time = datetime.now()
            for ix, entry in enumerate(squeue_entries):
                cursor.execute(
                    "INSERT INTO {} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)".format(table),
                    (
                        entry.job_id,
                        update_time,
                        entry.username,
                        entry.status,
                        entry.name,
                        entry.run_time,
                        entry.est_run_time,
                        entry.nodes,
                        entry.cores,
                    ),
                )

    def get_status_entry(self, hpc: const.HPC, id: int):
        """Gets the status entries for the specified HPC
        Note: Only Maui is currently supported
        """
        if hpc != const.HPC.maui:
            return None

        with self.get_cursor(self.db_file) as cursor:
            result = cursor.execute(
                "SELECT * FROM MAUI_CUR_STATUS WHERE ID = ?;", (id,)
            ).fetchone()

        return StatusEntry(*result)

    def update_status_entry(self, hpc: const.HPC, entry: StatusEntry):
        if hpc != const.HPC.maui:
            return None

        # Check if the entry exists
        with self.get_cursor(self.db_file) as cursor:
            result = cursor.execute(
                "SELECT * FROM MAUI_CUR_STATUS WHERE ID = ?", (entry.id,)
            ).fetchone()

            if result:
                cursor.execute(
                    "UPDATE MAUI_CUR_STATUS SET "
                    "INT_VALUE_1 = ?, INT_VALUE_2 = ?, UPDATE_TIME = ? WHERE ID == ?;",
                    (entry.int_value_1, entry.int_value_2, datetime.now(), entry.id),
                )
            else:
                cursor.execute(
                    "INSERT INTO MAUI_CUR_STATUS VALUES (?, ?, ?, ?, ?)",
                    (
                        entry.id,
                        entry.name,
                        entry.int_value_1,
                        entry.int_value_2,
                        datetime.now(),
                    ),
                )

    def get_squeue_entries(self, hpc: const.HPC):
        """Gets all squeue entries"""
        with self.get_cursor(self.db_file) as cursor:
            results = cursor.execute(
                "SELECT JOB_ID, USERNAME, STATUS, NAME, RUNTIME, \
                ESTIMATED_TIME, NODES, CORES  FROM {};".format(
                    self.get_squeue_t_name(hpc)
                )
            ).fetchall()

        return [SQueueEntry(*result) for result in results]

    def update_daily_quota(self, entry: QuotaEntry, hpc: const.HPC):
        if hpc != const.HPC.maui:
            return None

        day = date.today()

        with self.get_cursor(self.db_file) as cursor:
            result = cursor.execute(
                "SELECT * FROM MAUI_QUOTA WHERE FILE_SYSTEM = ? AND DATE = ?", (entry.file_system, day)
            ).fetchone()

            if result:
                cursor.execute(
                    "UPDATE MAUI_QUOTA SET "
                    "USED_SPACE = ?, AVAILABLE_INODES = ?, USED_INODES = ? WHERE ID == ?;",
                    (entry.used_space, entry.available_inodes, entry.used_inodes, entry.id),
                )
            else:
                cursor.execute(
                    "INSERT INTO MAUI_QUOTA VALUES (?, ?, ?, ?, ?)",
                    (
                        entry.file_system,
                        entry.used_space,
                        entry.available_inodes,
                        entry.used_inodes,
                        day,
                    ),
                )

    def get_daily_quota(self, hpc: const.HPC, file_system: str, day: Union[date, str] = date.today()):
        day = day.strftime(self.date_format) if type(day) is date else day
        with self.get_cursor(self.db_file) as cursor:
            results = cursor.execute(
                "SELECT * FROM {} WHERE FILE_SYSTEM = ? AND DAY = ?".format(self.get_quota_t_name(hpc)), (file_system, day)).fetchall()
        return results

    def _create_queue_table(self, cursor, hpc: const.HPC):
        # Add latest table
        cursor.execute(
            """CREATE TABLE IF NOT EXISTS {}(
                  JOB_ID INTEGER PRIMARY KEY NOT NULL,
                  UPDATE_TIME DATE NOT NULL,
                  USERNAME TEXT NOT NULL,
                  STATUS TEXT NOT NULL,
                  NAME TEXT NOT NULL,
                  RUNTIME FLOAT NOT NULL,
                  ESTIMATED_TIME FLOAT NOT NULL,
                  NODES INTEGER NOT NULL,
                  CORES INTEGER NOT NULL
                );
            """.format(
                self.get_squeue_t_name(hpc)
            )
        )

    def _create_quota_table(self, cursor, hpc: const.HPC):
        cursor.execute(
            """CREATE TABLE IF NOT EXISTS {}(
            ID INTEGER PRIMARY KEY AUTOINCREMENT,
            FILE_SYSTEM TEXT NOT NULL,
            USED_SPACE TEXT NOT NULL,
            AVAILABLE_INODES INTEGER NOT NULL,
            USED_INODES INTEGER NOT NULL,
            DAY DATE NOT NULL);
            """.format(self.get_quota_t_name(hpc))
        )

    @classmethod
    def create_db(
        cls, db_file: str, hpc: Union[const.HPC, Iterable[const.HPC]] = const.HPC
    ):
        """Creates a new Dashboard database, and returns instance of class"""
        if os.path.isfile(db_file):
            raise FileExistsError(
                "The specified database file {} already exists.".format(db_file)
            )

        with cls.get_cursor(db_file) as cursor:
            dashboard_db = DashboardDB(db_file)

            hpc = [hpc] if type(hpc) is const.HPC else hpc
            for cur_hpc in hpc:
                dashboard_db._create_queue_table(cursor, cur_hpc)
                dashboard_db._create_quota_table(cursor, cur_hpc)

                # Add daily table
                cursor.execute(
                    """CREATE TABLE IF NOT EXISTS {}(
                            DAY DATE PRIMARY KEY NOT NULL,
                            CORE_HOURS_USED FLOAT,
                            TOTAL_CORE_HOURS FLOAT,
                            UPDATE_TIME DATE NOT NULL
                      );
                    """.format(
                        cls.get_daily_t_name(cur_hpc)
                    )
                )

            # Maui current status
            cursor.execute(
                """CREATE TABLE IF NOT EXISTS MAUI_CUR_STATUS(
                    ID INT PRIMARY KEY NOT NULL, 
                    NAME TEXT NOT NULL, 
                    INT_VALUE_1 INT NULL, 
                    INT_VALUE_2 INT NULL,
                    UPDATE_TIME DATE NOT NULL)
                """
            )

        return dashboard_db

    @staticmethod
    @contextmanager
    def get_cursor(db_file):
        conn = sql.connect(db_file)
        try:
            yield conn.cursor()
        except Exception:
            conn.rollback()
            raise
        else:
            conn.commit()
        finally:
            conn.close()


