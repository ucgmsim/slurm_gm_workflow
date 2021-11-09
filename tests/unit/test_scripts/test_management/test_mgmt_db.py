import os
import shutil
import pytest
from scheduler.management.db_helper import connect_db_ctx
from scheduler.management import create_mgmt_db
from scheduler.management.MgmtDB import SchedulerTask
from qcore import utils
from qcore.qclogging import get_basic_logger

TEST_DB_FILE = "./output/slurm_mgmt.db"
TEST_SRF_FILE = "/nesi/nobackup/nesi00213/RunFolder/PangopangoF29/Data/Sources/PangopangoF29/Srf/PangopangoF29_HYP01-10_S1244.srf"
TEST_RUN_NAME = "PangopangoF29_HYP01-10_S1244"
TEST_PROC = (2, "merge_ts")
TEST_STATUS = (4, "completed")
FORCE_STATUS = (5, "failed")
INIT_DB_ROWS = 19
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
    assert len(get_rows(mgmt_db.db_file, "state", "proc_type", TEST_PROC[0])) == 2


def test_update_live_db(mgmt_db):
    mgmt_db.update_entries_live(
        [SchedulerTask(TEST_RUN_NAME, TEST_PROC[0], TEST_STATUS[0], None, None)],
        get_basic_logger(),
    )
    value = get_rows(
        mgmt_db.db_file, "state", "proc_type", TEST_PROC[0], selected_col="status"
    )[0][0]
    assert value == TEST_STATUS[0]

    mgmt_db.close_conn()


def teardown_module(module):
    shutil.rmtree(os.path.dirname(TEST_DB_FILE))
