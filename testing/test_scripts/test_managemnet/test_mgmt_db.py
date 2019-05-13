import os
import shutil
import pytest

import sqlite3 as sql

from scripts.management.db_helper import connect_db_ctx
from scripts.management import create_mgmt_db, update_mgmt_db
from scripts.management.MgmtDB import SlurmTask
from qcore import utils
from shared_workflow.workflow_logger import get_basic_logger

TEST_DB_FILE = "./output/slurm_mgmt.db"
TEST_SRF_FILE = "/nesi/nobackup/nesi00213/RunFolder/PangopangoF29/Data/Sources/PangopangoF29/Srf/PangopangoF29_HYP01-10_S1244.srf"
TEST_RUN_NAME = "PangopangoF29_HYP01-10_S1244"
TEST_PROC = (2, "merge_ts")
TEST_STATUS = (4, "completed")
FORCE_STATUS = (5, "failed")
INIT_DB_ROWS = 6
EXPECTED_ERROS = [
    "test_err",
    "",
    "Process failed retrying",
    "Reseting retries",
    "another error",
]


def setup_module(module):
    utils.setup_dir(os.path.dirname(TEST_DB_FILE))


@pytest.fixture(scope="module")
def mgmt_db():
    yield create_mgmt_db.create_mgmt_db([], TEST_DB_FILE, TEST_SRF_FILE)


def get_rows(db_file, table, col_name, col_value, selected_col="*"):
    query = "SELECT {} from {} where {} = ?".format(selected_col, table, col_name)

    with connect_db_ctx(db_file) as cur:
        rows = cur.execute(query, (col_value,)).fetchall()
    return rows


def test_create_mgmt_db(mgmt_db):
    assert (
        len(get_rows(mgmt_db.db_file, "state", "run_name", TEST_RUN_NAME))
        == INIT_DB_ROWS
    )


def test_insert_task(mgmt_db):
    mgmt_db.insert(TEST_RUN_NAME, TEST_PROC[0])
    assert len(get_rows(mgmt_db.db_file, "state", "proc_type", TEST_PROC[0])) == 1


def test_update_live_db(mgmt_db):
    mgmt_db.update_entries_live(
        [SlurmTask(TEST_RUN_NAME, TEST_PROC[0], TEST_STATUS[0], None, None)], get_basic_logger()
    )
    value = get_rows(
        mgmt_db.db_file, "state", "proc_type", TEST_PROC[0], selected_col="status"
    )[0][0]
    assert value == TEST_STATUS[0]

    mgmt_db.close_conn()


def test_update_db_error(mgmt_db):
    with connect_db_ctx(mgmt_db.db_file) as cur:
        update_mgmt_db.update_error(
            cur, TEST_PROC[1], run_name=TEST_RUN_NAME, error="test_err"
        )

    value = get_rows(
        mgmt_db.db_file, "error", "task_id", TEST_PROC[0], selected_col="error"
    )[0][0]
    assert value == "test_err"


def test_update_task_time(mgmt_db):
    with connect_db_ctx(mgmt_db.db_file) as cur:
        update_mgmt_db.update_task_time(
            cur, TEST_RUN_NAME, TEST_PROC[1], TEST_STATUS[1]
        )

    test_time = get_rows(
        mgmt_db.db_file, "task_time_log", "state_id", TEST_PROC[0], selected_col="time"
    )[0][0]
    bench_time = get_rows(
        mgmt_db.db_file,
        "state",
        "proc_type",
        TEST_PROC[0],
        selected_col="last_modified",
    )[0][0]
    assert test_time == bench_time


def test_force_update_db(mgmt_db):
    with connect_db_ctx(mgmt_db.db_file) as cur:
        update_mgmt_db.force_update_db(
            cur, TEST_PROC[1], FORCE_STATUS[1], run_name=TEST_RUN_NAME
        )

    value = get_rows(
        mgmt_db.db_file, "state", "proc_type", TEST_PROC[0], selected_col="status"
    )[0][0]
    assert value == FORCE_STATUS[0]


def test_force_update_db_error(mgmt_db):
    with connect_db_ctx(mgmt_db.db_file) as cur:
        update_mgmt_db.force_update_db(
            cur,
            TEST_PROC[1],
            FORCE_STATUS[1],
            run_name=TEST_RUN_NAME,
            retry=True,
            reset_retries=True,
            error="another error",
        )

    rows = get_rows(
        mgmt_db.db_file, "error", "task_id", TEST_PROC[0], selected_col="error"
    )
    errors = []
    for row in rows:
        for err in row:
            errors.append(err)
    assert errors == EXPECTED_ERROS


def teardown_module(module):
    shutil.rmtree(os.path.dirname(TEST_DB_FILE))
