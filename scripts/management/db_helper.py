import os
import sqlite3 as sql
from contextlib import contextmanager
from collections import namedtuple


def connect_db(path):
    db_location = os.path.abspath(os.path.join(path, "slurm_mgmt.db"))
    conn = sql.connect(db_location)
    db = conn.cursor()
    db.execute("PRAGMA synchronous = EXTRA")
    # db.execute("PRAGMA synchronous = OFF")
    db.execute("PRAGMA integrity_check")

    return db


@contextmanager
def connect_db_ctx(db_file):
    """Returns a db cursor. Use with a context (i.e. with statement)

    A commit is run at the end of the context.
    """
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


SlurmTask = namedtuple(
    "SlurmTask", ["run_name", "proc_type", "status", "job_id", "retries"]
)


def enum_to_list(enum):
    return [x.name for x in enum]
