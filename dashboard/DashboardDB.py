import os
import shutil
import unittest
import sqlite3 as sql
from collections import namedtuple
from typing import Iterable, Union
from datetime import datetime, date, timedelta

SQueueEntry = namedtuple(
    "SQueue",
    ["job_id", "username", "status", "run_time", "est_run_time", "nodes", "cores"],
)


class DashboardDB:

    date_format = "%d-%m-%Y"

    def __init__(self, db_file: str):
        """Opens an existing dashboard database file."""

        if not os.path.isfile(db_file):
            raise FileNotFoundError(
                "Specified database {} is not a valid file.".format(db_file)
            )

        self.db_file = db_file

    def update_daily_chours_usage(
        self, total_core_usage: float, day: Union[date, str] = date.today()
    ):
        """Updates the daily core hours usage.

        The core hour usage for a day is calculated as
        last saved (for current day) TOTAL_CORE_USAGE - current total_core_usage.
        """
        conn = sql.connect(self.db_file)
        cursor = conn.cursor()

        day = day.strftime(self.date_format) if type(day) is date else day
        row = cursor.execute(
            "SELECT * FROM MAUI_DAILY WHERE DAY == ?;", (day,)
        ).fetchone()

        # New day
        if row is None:
            cursor.execute(
                "INSERT INTO MAUI_DAILY VALUES(?, ?, ?)", (day, None, total_core_usage)
            )
        else:
            chours_usage = total_core_usage - row[2]
            cursor.execute(
                "UPDATE MAUI_DAILY SET CORE_HOURS_USED = ? WHERE DAY == ?;",
                (chours_usage, day),
            )
            cursor.execute(
                "UPDATE MAUI_DAILY SET TOTAL_CORE_HOURS = ? WHERE DAY == ?;",
                (total_core_usage, day),
            )

        conn.commit()
        conn.close()

    def get_daily_chours_usage(self, day: Union[date, str]):
        """Gets the usage and total usage for the specified day"""
        conn = sql.connect(self.db_file)
        cursor = conn.cursor()

        day = day.strftime(self.date_format) if type(day) is date else day
        result = cursor.execute(
            "SELECT CORE_HOURS_USED, TOTAL_CORE_HOURS FROM MAUI_DAILY WHERE DAY == ?",
            (day,),
        ).fetchone()

        conn.close()
        return result

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
                "INSERT INTO MAUI_SQUEUE \
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    entry.job_id,
                    update_time,
                    entry.username,
                    entry.status,
                    entry.run_time,
                    entry.est_run_time,
                    entry.nodes,
                    entry.cores,
                ),
            )

        conn.commit()
        conn.close()

    def get_sqeue_entries(self):
        """Gets all squeue entries"""
        conn = sql.connect(self.db_file)
        cursor = conn.cursor()

        results = cursor.execute(
            "SELECT JOB_ID, USERNAME, STATUS, RUNTIME, \
            ESTIMATED_TIME, NODES, CORES  FROM MAUI_SQUEUE;"
        ).fetchall()

        conn.close()
        return [SQueueEntry(*result) for result in results]

    def _create_queue_table(self, cursor):
        # Add latest table
        cursor.execute(
            """CREATE TABLE MAUI_SQUEUE(
                  JOB_ID INTEGER PRIMARY KEY NOT NULL,
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

    squeue_entry_1 = SQueueEntry(12, "testUser", "running", 0.5, 0.8, 1, 80)
    squeue_entry_2 = SQueueEntry(13, "testUser", "running", 0.4, 0.8, 1, 80)
    squeue_entry_3 = SQueueEntry(14, "testUser", "running", 0.5, 0.8, 1, 80)

    total_core_usage_1 = 150
    total_core_usage_2 = 213

    @classmethod
    def setUpClass(cls):
        """Createst a test database (only once for all tests), this can be changed to
        per test if required
        """
        os.mkdir(cls.test_dir)
        DashboardDB.create_db(cls.test_db)

    def test_squeue(self):
        """Test updating/populating of squeue table

        Tests population of empty squeue table
        and population of already populated squeue table (i.e. drop & recreate)"""
        db = DashboardDB(self.test_db)

        # Add entries to empty db
        initial_entries = [self.squeue_entry_1, self.squeue_entry_2]
        db.update_squeue(initial_entries)

        cur_entries = db.get_sqeue_entries()

        # Check that they were added
        self.assertTrue(len(cur_entries), len(initial_entries))
        for entry in initial_entries:
            self.assertIn(entry, cur_entries)

        # Add entries to non-empty db
        db.update_squeue([self.squeue_entry_3])

        # Check that table was dropped, re-created and then new entry added
        cur_entries = db.get_sqeue_entries()

        self.assertTrue(len(cur_entries), 1)
        self.assertEqual(cur_entries[0], self.squeue_entry_3)

    def test_daily(self):
        """Test inserting to daily table

        Insert into daily for first time
                - check daily usage is null
            Insert 2nd time
                - check daily usage updates

            Add entry for different day,
            make sure this creates a new entry
        """
        db = DashboardDB(self.test_db)

        # Add first entry for the day 1
        day_1 = date.today().strftime(DashboardDB.date_format)
        db.update_daily_chours_usage(self.total_core_usage_1, day_1)

        # Check the entry has been added & daily usage is null
        daily_usage, total_core_hours = db.get_daily_chours_usage(day_1)
        self.assertIsNone(daily_usage)
        self.assertEqual(total_core_hours, self.total_core_usage_1)

        # Add 2nd entry for day 1
        db.update_daily_chours_usage(self.total_core_usage_2, day_1)

        # Add entry for different day
        day_2 = (date.today() + timedelta(days=1)).strftime(DashboardDB.date_format)
        db.update_daily_chours_usage(self.total_core_usage_2, day_2)

        # Check that entry for day 1 has been updated correctly
        daily_usage, total_core_hours = db.get_daily_chours_usage(day_1)
        self.assertEqual(daily_usage, self.total_core_usage_2 - self.total_core_usage_1)
        self.assertEqual(total_core_hours, self.total_core_usage_2)

        # Check that new entry has been created for day 2
        daily_usage, total_core_hours = db.get_daily_chours_usage(day_2)
        self.assertIsNone(daily_usage)
        self.assertEqual(total_core_hours, self.total_core_usage_2)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.test_dir)


if __name__ == "__main__":
    unittest.main()
