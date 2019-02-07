import os
import sqlite3
from enum import Enum


def connect_db(path):
    db_location = os.path.abspath(os.path.join(path, 'slurm_mgmt.db'))
    conn = sqlite3.connect(db_location)
    db = conn.cursor()
    db.execute("PRAGMA synchronous = EXTRA")
    #db.execute("PRAGMA synchronous = OFF")
    db.execute("PRAGMA integrity_check")

    return db


class State(Enum):
    created = 1
    queued = 2
    running = 3
    completed = 4
    failed = 5

class Task:

    def __init__(self, process, state, run_name):
        self.process = process
        self.state = state
        self.run_name = run_name


def enum_to_list(enum):
    return [x.name for x in enum]
