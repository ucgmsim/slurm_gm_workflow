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


class Process(Enum):
    EMOD3D = 1
    merge_ts = 2
    winbin_aio = 3
    HF = 4
    BB = 5
    IM_calculation = 6
    IM_plot = 7
    Empirical = 8
