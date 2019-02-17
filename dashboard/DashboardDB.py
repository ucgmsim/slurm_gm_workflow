import os
import shutil
import unittest
import sqlite3 as sql
from collections import namedtuple
from typing import Iterable, Union
from datetime import datetime, date, timedelta

import qcore.constants as const

SQueueEntry = namedtuple(
    "SQueue",
    ["job_id", "username", "status", "name", "run_time", "est_run_time", "nodes", "cores"],
)


class DashboardDB:

    date_format = "%d-%m-%Y"

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

        conn = sql.connect(self.db_file)
        cursor = conn.cursor()

        table = self.get_daily_t_name(hpc)
        day = day.strftime(self.date_format) if type(day) is date else day
        row = cursor.execute(
            "SELECT CORE_HOURS_USED, TOTAL_CORE_HOURS FROM {} WHERE DAY == ?;".format(table), (day,)
        ).fetchone()

        # New day
        update_time = datetime.now()
        if row is None:
            cursor.execute(
                "INSERT INTO {} VALUES(?, ?, ?, ?)".format(table),
                (day, 0, total_core_usage, update_time),
            )
        else:
            chours_usage =  total_core_usage - row[1] if row[1] is not None else 0
            if row[0] is not None:
                chours_usage = row[0] + chours_usage

            cursor.execute(
                "UPDATE {} SET CORE_HOURS_USED = ?, TOTAL_CORE_HOURS = ?, \
                UPDATE_TIME = ? WHERE DAY == ?;".format(table),
                (chours_usage, total_core_usage, update_time, day),
            )

        conn.commit()
        conn.close()

    def get_daily_chours_usage(self, day: Union[date, str], hpc: const.HPC):
        """Gets the usage and total usage for the specified day"""
        conn = sql.connect(self.db_file)
        cursor = conn.cursor()

        day = day.strftime(self.date_format) if type(day) is date else day
        result = cursor.execute(
            "SELECT CORE_HOURS_USED, TOTAL_CORE_HOURS FROM {} WHERE DAY == ?".format(
                self.get_daily_t_name(hpc)
            ),
            (day,),
        ).fetchone()

        conn.close()
        return result

    def update_squeue(self, squeue_entries: Iterable[SQueueEntry], hpc: const.HPC):
        """Updates the squeue table with the latest queue status"""
        conn = sql.connect(self.db_file)
        cursor = conn.cursor()

        table = self.get_squeue_t_name(hpc)

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

        conn.commit()
        conn.close()

    def get_sqeue_entries(self, hpc: const.HPC):
        """Gets all squeue entries"""
        conn = sql.connect(self.db_file)
        cursor = conn.cursor()

        results = cursor.execute(
            "SELECT JOB_ID, USERNAME, STATUS, NAME, RUNTIME, \
            ESTIMATED_TIME, NODES, CORES  FROM {};".format(
                self.get_squeue_t_name(hpc)
            )
        ).fetchall()

        conn.close()
        return [SQueueEntry(*result) for result in results]

    def _create_queue_table(self, cursor, hpc: const.HPC):
        # Add latest table
        cursor.execute(
            """CREATE TABLE {}(
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

    @classmethod
    def create_db(
        cls, db_file: str, hpc: Union[const.HPC, Iterable[const.HPC]] = const.HPC
    ):
        """Creates a new Dashboard database, and returns instance of class"""
        if os.path.isfile(db_file):
            raise FileExistsError(
                "The specified database file {} already exists.".format(db_file)
            )

        conn = sql.connect(db_file)
        cursor = conn.cursor()

        dashboard_db = DashboardDB(db_file)

        hpc = [hpc] if type(hpc) is const.HPC else hpc
        for cur_hpc in hpc:
            dashboard_db._create_queue_table(cursor, cur_hpc)

            # Add daily table
            cursor.execute(
                """CREATE TABLE {}(
                        DAY DATE PRIMARY KEY NOT NULL,
                        CORE_HOURS_USED FLOAT,
                        TOTAL_CORE_HOURS FLOAT,
                        UPDATE_TIME DATE NOT NULL
                  );
                """.format(
                    cls.get_daily_t_name(cur_hpc)
                )
            )

        conn.commit()
        conn.close()

        return dashboard_db


######################### Tests #########################


class TestDashboardDB(unittest.TestCase):

    test_dir = "./tests"
    test_db = os.path.join(test_dir, "test.db")

    squeue_entry_1 = SQueueEntry(12, "testUser", "R", "testName_1", "1:20", "16:20", 1, 80)
    squeue_entry_2 = SQueueEntry(13, "testUser", "R", "testName_2", "11:20", "16:16:20", 1, 80)
    squeue_entry_3 = SQueueEntry(14, "testUser", "R", "testName_3", "14:20", "12:16:20", 1, 80)

    hpc_1 = const.HPC.maui
    hpc_2 = const.HPC.mahuika

    total_core_usage_1 = 150
    total_core_usage_2 = 213

    @classmethod
    def setUpClass(cls):
        """Createst a test database (only once for all tests), this can be changed to
        per test if required
        """
        os.mkdir(cls.test_dir)
        DashboardDB.create_db(cls.test_db, [cls.hpc_1, cls.hpc_2])

    def test_squeue_hpc_1(self):
        """Test updating/populating of squeue table"""
        self.check_squeue(self.hpc_1)

    def test_squeue_hpc_2(self):
        """Test updating/populating of squeue table"""
        self.check_squeue(self.hpc_2)

    def check_squeue(self, hpc: const.HPC):
        """Tests population of empty squeue table
        and population of already populated squeue table (i.e. drop & recreate)"""
        db = DashboardDB(self.test_db)

        # Add entries to empty db
        initial_entries = [self.squeue_entry_1, self.squeue_entry_2]
        db.update_squeue(initial_entries, hpc)

        cur_entries = db.get_sqeue_entries(hpc)

        # Check that they were added
        self.assertTrue(len(cur_entries), len(initial_entries))
        for entry in initial_entries:
            self.assertIn(entry, cur_entries)

        # Add entries to non-empty db
        db.update_squeue([self.squeue_entry_3], hpc)

        # Check that table was dropped, re-created and then new entry added
        cur_entries = db.get_sqeue_entries(hpc)

        self.assertTrue(len(cur_entries), 1)
        self.assertEqual(cur_entries[0], self.squeue_entry_3)

    def test_daily_hpc_1(self):
        """Test inserting to daily table"""
        self.check_daily(self.hpc_1)

    def test_daily_hpc_2(self):
        """Test inserting to daily table"""
        self.check_daily(self.hpc_2)

    def check_daily(self, hpc):
        """Insert into daily for first time
                - check daily usage is null
            Insert 2nd time
                - check daily usage updates

            Add entry for different day,
            make sure this creates a new entry
        """
        db = DashboardDB(self.test_db)

        # Add first entry for the day 1
        day_1 = date.today().strftime(DashboardDB.date_format)
        db.update_daily_chours_usage(self.total_core_usage_1, hpc, day_1)

        # Check the entry has been added & daily usage is zero
        daily_usage, total_core_hours = db.get_daily_chours_usage(day_1, hpc)
        self.assertEqual(daily_usage, 0)
        self.assertEqual(total_core_hours, self.total_core_usage_1)

        # Add 2nd entry for day 1
        db.update_daily_chours_usage(self.total_core_usage_2, hpc, day_1)

        # Add entry for different day
        day_2 = (date.today() + timedelta(days=1)).strftime(DashboardDB.date_format)
        db.update_daily_chours_usage(self.total_core_usage_2, hpc, day_2)

        # Check that entry for day 1 has been updated correctly
        daily_usage, total_core_hours = db.get_daily_chours_usage(day_1, hpc)
        self.assertEqual(daily_usage, self.total_core_usage_2 - self.total_core_usage_1)
        self.assertEqual(total_core_hours, self.total_core_usage_2)

        # Check that new entry has been created for day 2
        daily_usage, total_core_hours = db.get_daily_chours_usage(day_2, hpc)
        self.assertEqual(daily_usage, 0)
        self.assertEqual(total_core_hours, self.total_core_usage_2)

    def test_daily_same_total_chours(self):
        """Tests that when core hours has a value and multiple update calls are made
        with the same total_core_hours"""
        db = DashboardDB(self.test_db)

        # Add first entry for the day 1
        day_1 = date.today().strftime(DashboardDB.date_format)
        db.update_daily_chours_usage(self.total_core_usage_1, self.hpc_1, day_1)

        # Check the entry has been added & daily usage is null
        daily_usage, total_core_hours = db.get_daily_chours_usage(day_1, self.hpc_1)
        self.assertEqual(daily_usage, 0)
        self.assertEqual(total_core_hours, self.total_core_usage_1)

        # Add 2nd entry for day 1
        db.update_daily_chours_usage(self.total_core_usage_2, self.hpc_1, day_1)

        # Check that entry for day 1 has been updated correctly
        daily_usage, total_core_hours = db.get_daily_chours_usage(day_1, self.hpc_1)
        self.assertEqual(daily_usage, self.total_core_usage_2 - self.total_core_usage_1)
        self.assertEqual(total_core_hours, self.total_core_usage_2)

        # Add 2nd entry for day 1 again
        db.update_daily_chours_usage(self.total_core_usage_2, self.hpc_1, day_1)

        # Check that nothing has changed
        daily_usage, total_core_hours = db.get_daily_chours_usage(day_1, self.hpc_1)
        self.assertEqual(daily_usage, self.total_core_usage_2 - self.total_core_usage_1)
        self.assertEqual(total_core_hours, self.total_core_usage_2)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.test_dir)


if __name__ == "__main__":
    unittest.main()
