import shutil
import pytest

from scripts.management import create_mgmt_db, update_mgmt_db
from qcore import utils

TEST_DB_DIR = "output"
TEST_SRF_FILE = "/nesi/nobackup/nesi00213/RunFolder/PangopangoF29/Data/Sources/PangopangoF29/Srf/PangopangoF29_HYP01-10_S1244.srf"
TEST_RUN_NAME = "PangopangoF29_HYP01-10_S1244"
TEST_PROC = (2, "merge_ts")
TEST_STATUS = (4, "completed")
FORCE_STATUS = (5, "failed")
INIT_DB_ROWS = 10
EXPECTED_ERROS = [
    "test_err",
    "",
    "Process failed retrying",
    "Reseting retries",
    "another error",
]


def setup_module(module):
    utils.setup_dir(TEST_DB_DIR)


@pytest.fixture(scope="module")
def mgmt_db():
    yield create_mgmt_db.create_mgmt_db([], TEST_DB_DIR, TEST_SRF_FILE)


def get_rows(mgmt_db, table, col_name, col_value, selected_col="*"):
    sql = "SELECT {} from {} where {} = ?".format(selected_col, table, col_name)
    rows = mgmt_db.execute(sql, (col_value,)).fetchall()
    return rows


def test_create_mgmt_db(mgmt_db):
    assert len(get_rows(mgmt_db, "state", "run_name", TEST_RUN_NAME)) == INIT_DB_ROWS


def test_get_procs(mgmt_db):
    proc_types = create_mgmt_db.get_procs(mgmt_db)
    assert len(proc_types) == INIT_DB_ROWS
    assert proc_types[5][-1] == "IM_calculation"


def test_insert_task(mgmt_db):
    create_mgmt_db.insert_task(mgmt_db, TEST_RUN_NAME, TEST_PROC[0])
    assert len(get_rows(mgmt_db, "state", "proc_type", TEST_PROC[0])) == 1


def test_update_db(mgmt_db):
    update_mgmt_db.update_db(
        mgmt_db, TEST_PROC[1], TEST_STATUS[1], run_name=TEST_RUN_NAME
    )
    value = get_rows(
        mgmt_db, "state", "proc_type", TEST_PROC[0], selected_col="status"
    )[0][0]
    assert value == TEST_STATUS[0]


def test_update_db_error(mgmt_db):
    update_mgmt_db.update_error(
        mgmt_db, TEST_PROC[1], run_name=TEST_RUN_NAME, error="test_err"
    )
    value = get_rows(mgmt_db, "error", "task_id", TEST_PROC[0], selected_col="error")[
        0
    ][0]
    assert value == "test_err"


def test_update_task_time(mgmt_db):
    update_mgmt_db.update_task_time(
        mgmt_db, TEST_RUN_NAME, TEST_PROC[1], TEST_STATUS[1]
    )
    test_time = get_rows(
        mgmt_db, "task_time_log", "state_id", TEST_PROC[0], selected_col="time"
    )[0][0]
    bench_time = get_rows(
        mgmt_db, "state", "proc_type", TEST_PROC[0], selected_col="last_modified"
    )[0][0]
    assert test_time == bench_time


def test_force_update_db(mgmt_db):
    update_mgmt_db.force_update_db(
        mgmt_db, TEST_PROC[1], FORCE_STATUS[1], run_name=TEST_RUN_NAME
    )
    value = get_rows(
        mgmt_db, "state", "proc_type", TEST_PROC[0], selected_col="status"
    )[0][0]
    assert value == FORCE_STATUS[0]


def test_force_update_db_error(mgmt_db):
    update_mgmt_db.force_update_db(
        mgmt_db,
        TEST_PROC[1],
        FORCE_STATUS[1],
        run_name=TEST_RUN_NAME,
        retry=True,
        reset_retries=True,
        error="another error",
    )
    rows = get_rows(mgmt_db, "error", "task_id", TEST_PROC[0], selected_col="error")
    errors = []
    for row in rows:
        for err in row:
            errors.append(err)
    assert errors == EXPECTED_ERROS


def teardown_module(module):
    shutil.rmtree(TEST_DB_DIR)
