import os
import sqlite3
from enum import Enum


def connect_db(path):
    db_location = os.path.join(path, 'slurm_mgmt.db')
    conn = sqlite3.connect(db_location)
    db = conn.cursor()
    db.execute("PRAGMA synchronous = OFF")
    return db


class State(Enum):
    created = 1
    queued = 2
    running = 3
    completed = 4
    failed = 5


# Process 1-5 are simulation 6-7 are Intensity Measure and 8-10 are simulation verification
class Process(Enum):
    EMOD3D = 1
    merge_ts = 2
    winbin_aio = 3
    HF = 4
    BB = 5
    IM_calculation = 6
    IM_plot = 7
    rrup = 8
    Empirical = 9
    Verification = 10


class Task:

    def __init__(self, process, state, run_name):
        self.process = process
        self.state = state
        self.run_name = run_name


def enum_to_list(enum):
    return [x.name for x in enum]
