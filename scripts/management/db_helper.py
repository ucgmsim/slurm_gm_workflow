import os
import sqlite3


def connect_db(path):
    print("path",path)
    db_location = os.path.abspath(os.path.join(path, 'slurm_mgmt.db'))
    conn = sqlite3.connect(db_location)
    db = conn.cursor()
    db.execute("PRAGMA synchronous = EXTRA")
    #db.execute("PRAGMA synchronous = OFF")
    db.execute("PRAGMA integrity_check")

    return db


class Task:

    def __init__(self, process, state, run_name):
        self.process = process
        self.state = state
        self.run_name = run_name


def enum_to_list(enum):
    return [x.name for x in enum]
