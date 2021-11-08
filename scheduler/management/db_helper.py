import os
import sqlite3 as sql
from contextlib import contextmanager


def connect_db(path):
    db_location = os.path.abspath(os.path.join(path, "slurm_mgmt.db"))
    conn = sql.connect(db_location)
    db = conn.cursor()
    db.execute("PRAGMA synchronous = EXTRA")
    # db.execute("PRAGMA synchronous = OFF")
    db.execute("PRAGMA integrity_check")

    return db


@contextmanager
def connect_db_ctx(db_file, verbose=False):
    """Returns a db cursor. Use with a context (i.e. with statement)

    A commit is run at the end of the context.
    """
    conn = sql.connect(db_file)
    if verbose:
        conn.set_trace_callback(print)

    try:
        yield conn.cursor()
    except Exception:
        conn.rollback()
        raise
    else:
        conn.commit()
    finally:
        conn.close()


def enum_to_list(enum):
    return [x.name for x in enum]
