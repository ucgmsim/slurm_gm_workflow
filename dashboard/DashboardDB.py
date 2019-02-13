import os
import shutil
import unittest
import sqlite3 as sql
from collections import namedtuple
from typing import Iterable
from datetime import datetime, date

SQueueEntry = namedtuple(
    "SQueue", ["username", "status", "run_time", "est_run_time", "nodes", "cores"]
)


class DashboardDB:
    def __init__(self, db_file: str):
        """Opens an existing dashboard database file."""

        if not os.path.isfile(db_file):
            raise FileNotFoundError(
                "Specified database {} is not a valid file.".format(db_file)
            )

        self.db_file = db_file

    def update_daily_chours_usage(self, total_core_usage):
        """Updates the daily core hours usage.

        The core hour usage for a day is calculated as
        last saved (for current day) TOTAL_CORE_USAGE - current total_core_usage.
        """
        conn = sql.connect(self.db_file)
        cursor = conn.cursor()

        today = date.today().strftime("%d-%m-%Y")
        row = cursor.execute(
            """SELECT * FROM MAUI_DAILY WHERE DAY == '{}';""".format(today)
        ).fetchone()

        # New day
        if row is None:
            cursor.execute(
                """INSERT INTO MAUI_DAILY VALUES({}, {}, {})""".format(
                    today, None, total_core_usage
                )
            )
        else:
            chours_usage = total_core_usage - row[2]
            cursor.execute(
                """UPDATE MAUI_DAILY SET CORE_HOURS_USED = {} WHERE DAY == '{}';""".format(
                    chours_usage, today))

    def update_squeue(self, squeue_entries: Iterable[SQueueEntry]):
        """Updates the squeue table with the latest queue status"""
        conn = sql.connect(self.db_file)
        cursor = conn.cursor()

        # Drop and re-create the table
        cursor.execute("""DROP TABLE IF EXISTS MAUI_SQUEUE""")
        self._create_queue_table(cursor)

        update_time = datetime.now()
        for ix, entry in enumerate(squeue_entries):
            cursor.execute(
                """INSERT INTO MAUI_SQUEUE VALUES ()""".format(
                    ix,
                    update_time,
                    entry.username,
                    entry.status,
                    entry.run_time,
                    entry.est_run_time,
                    entry.nodes,
                    entry.cores,
                )
            )

        conn.commit()
        conn.close()

    def _create_queue_table(self, cursor):
        # Add latest table
        cursor.execute(
            """CREATE TABLE MAUI_SQUEUE(
                  ID INTEGER PRIMARY KEY NOT NULL,
                  UPDATE_TIME DATE NOT NULL,
                  USERNAME TEXT NOT NULL,
                  STATUS TEXT NOT NULL,
                  RUNTIME FLOAT NOT NULL,
                  ESTIMATED_TIME FLOAT NOT NULL,
                  NODES INTEGER NOT NULL,
                  CORES INTEGER NOT NULL
                );
            """
        )

    @classmethod
    def create_db(cls, db_file: str):
        """Creates a new Dashboard database, and returns instance of class"""
        if os.path.isfile(db_file):
            raise FileExistsError(
                "The specified database file {} already exists.".format(db_file)
            )

        conn = sql.connect(db_file)
        cursor = conn.cursor()

        dashboard_db = DashboardDB(db_file)
        dashboard_db._create_queue_table(cursor)

        # Add daily table
        cursor.execute(
            """CREATE TABLE MAUI_DAILY(
                    DAY DATE PRIMARY KEY NOT NULL,
                    CORE_HOURS_USED FLOAT,
                    TOTAL_CORE_HOURS FLOAT
              );
            """
        )

        conn.commit()
        conn.close()

        return dashboard_db


######################### Tests #########################

class TestDashboardDB(unittest.TestCase):

    test_dir = "./tests"
    test_db = os.path.join(test_dir, "test.db")

    @classmethod
    def setUpClass(cls):
        """Createst a test database (only once for all tests), this can be changed to
        per test if required
        """
        DashboardDB.create_db(cls.test_db)

    def

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.test_dir)

