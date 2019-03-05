import sys
import os
from datetime import date, timedelta

import pytest

import qcore.constants as const

sys.path.insert(0, os.path.join(os.path.abspath(os.path.dirname(__file__)), '../..'))
from dashboard.DashboardDB import DashboardDB, SQueueEntry


class TestDashboardDB:

    squeue_entry_1 = SQueueEntry(
        12, "testUser", "R", "testName_1", "1:20", "16:20", 1, 80
    )
    squeue_entry_2 = SQueueEntry(
        13, "testUser", "R", "testName_2", "11:20", "16:16:20", 1, 80
    )
    squeue_entry_3 = SQueueEntry(
        14, "testUser", "R", "testName_3", "14:20", "12:16:20", 1, 80
    )

    hpc_1 = const.HPC.maui
    hpc_2 = const.HPC.mahuika

    total_core_usage_1 = 150
    total_core_usage_2 = 213

    @pytest.fixture(scope="function")
    def dashboard_db(self, tmp_path):
        return DashboardDB.create_db(
            os.path.join(tmp_path, "test.db"), [self.hpc_1, self.hpc_2]
        )

    def test_squeue_hpc_1(self, dashboard_db: DashboardDB):
        """Test updating/populating of squeue table"""
        self.check_squeue(dashboard_db, self.hpc_1)

    def test_squeue_hpc_2(self, dashboard_db: DashboardDB):
        """Test updating/populating of squeue table"""
        self.check_squeue(dashboard_db, self.hpc_2)

    def check_squeue(self, db: DashboardDB, hpc: const.HPC):
        """Tests population of empty squeue table
        and population of already populated squeue table (i.e. drop & recreate)"""
        # Add entries to empty db
        initial_entries = [self.squeue_entry_1, self.squeue_entry_2]
        db.update_squeue(initial_entries, hpc)

        cur_entries = db.get_squeue_entries(hpc)

        # Check that they were added
        assert len(cur_entries) == len(initial_entries)
        for entry in initial_entries:
            assert entry in cur_entries

        # Add entries to non-empty db
        db.update_squeue([self.squeue_entry_3], hpc)

        # Check that table was dropped, re-created and then new entry added
        cur_entries = db.get_squeue_entries(hpc)
        assert len(cur_entries) == 1
        assert cur_entries[0] == self.squeue_entry_3

    def test_daily_hpc_1(self, dashboard_db: DashboardDB):
        """Test inserting to daily table"""
        self.check_daily(dashboard_db, self.hpc_1)

    def test_daily_hpc_2(self, dashboard_db: DashboardDB):
        """Test inserting to daily table"""
        self.check_daily(dashboard_db, self.hpc_2)

    def check_daily(self, db, hpc):
        """Insert into daily for first time
                - check daily usage is null
            Insert 2nd time
                - check daily usage updates

            Add entry for different day,
            make sure this creates a new entry
        """
        # Add first entry for the day 1
        day_1 = date.today().strftime(DashboardDB.date_format)
        db.update_daily_chours_usage(self.total_core_usage_1, hpc, day_1)

        # Check the entry has been added & daily usage is zero
        day, daily_usage, total_core_hours = db.get_chours_usage(day_1, day_1, hpc)[0]
        assert day == day_1
        assert daily_usage == 0
        assert total_core_hours == self.total_core_usage_1

        # Add 2nd entry for day 1
        db.update_daily_chours_usage(self.total_core_usage_2, hpc, day_1)

        # Add entry for different day
        day_2 = (date.today() + timedelta(days=1)).strftime(DashboardDB.date_format)
        db.update_daily_chours_usage(self.total_core_usage_2, hpc, day_2)

        # Check that entry for day 1 has been updated correctly
        day, daily_usage, total_core_hours = db.get_chours_usage(day_1, day_1, hpc)[0]
        assert day == day_1
        assert daily_usage == self.total_core_usage_2 - self.total_core_usage_1
        assert total_core_hours == self.total_core_usage_2

        # Check that new entry has been created for day 2
        day, daily_usage, total_core_hours = db.get_chours_usage(day_2, day_2, hpc)[0]
        assert day == day_2
        assert daily_usage == 0
        assert total_core_hours == self.total_core_usage_2

    def test_daily_same_total_chours(self, dashboard_db: DashboardDB):
        """Tests that when core hours has a value and multiple update calls are made
        with the same total_core_hours"""
        # Add first entry for the day 1
        day_1 = date.today().strftime(DashboardDB.date_format)
        dashboard_db.update_daily_chours_usage(
            self.total_core_usage_1, self.hpc_1, day_1
        )

        # Check the entry has been added & daily usage is null
        day, daily_usage, total_core_hours = dashboard_db.get_chours_usage(
            day_1, day_1, self.hpc_1
        )[0]
        assert day == day_1
        assert daily_usage == 0
        assert total_core_hours == self.total_core_usage_1

        # Add 2nd entry for day 1
        dashboard_db.update_daily_chours_usage(
            self.total_core_usage_2, self.hpc_1, day_1
        )

        # Check that entry for day 1 has been updated correctly
        day, daily_usage, total_core_hours = dashboard_db.get_chours_usage(
            day_1, day_1, self.hpc_1
        )[0]
        assert day == day_1
        assert daily_usage == self.total_core_usage_2 - self.total_core_usage_1
        assert total_core_hours == self.total_core_usage_2

        # Add 2nd entry for day 1 again
        dashboard_db.update_daily_chours_usage(
            self.total_core_usage_2, self.hpc_1, day_1
        )

        # Check that nothing has changed
        day, daily_usage, total_core_hours = dashboard_db.get_chours_usage(
            day_1, day_1, self.hpc_1
        )[0]
        assert day == day_1
        assert daily_usage == self.total_core_usage_2 - self.total_core_usage_1
        assert total_core_hours == self.total_core_usage_2


if __name__ == "__main__":
    pytest.main(sys.argv)
