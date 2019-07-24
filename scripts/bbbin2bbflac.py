#!/usr/bin/env python3
"""
Single process, single task .

Known working binary versions:
sox 14.4.2
flac 1.3.2
"""

from argparse import ArgumentParser
from base64 import b64encode
import os
from subprocess import call

import numpy as np

from qcore.timeseries import BBSeis

MAX_INT = np.float64(0x7FFFFC00)

parser = ArgumentParser()
parser.add_argument("bb_bin", help="path to binary broadband file")
parser.add_argument("bb_flac", help="path to flac broadband file")
args = parser.parse_args()
assert os.path.isfile(args.bb_bin)

bb = BBSeis(args.bb_bin)
sample_rate = int(round(1.0 / bb.dt))
# temp files
raw32 = args.bb_flac + ".f32le"
raw24 = args.bb_flac + ".f24le"
stat64 = args.bb_flac + ".b64stat"

# extract station data
stat_dtype = bb.stations.dtype.descr
stat_dtype[2] = ("name", "|S7")
stat_dtype.append(("scale", np.float64))
stations = np.empty(bb.stations.size, dtype=stat_dtype)
for col in bb.stations.dtype.names:
    stations[col] = bb.stations[col]

# extract scaled binary data
with open(raw32, "wb") as d:
    for i, s in enumerate(bb.stations.name):
        acc = bb.acc(s).astype(np.float64)
        stations["scale"][i] = MAX_INT / np.max(np.abs(acc))
        np.round(acc * stations["scale"][i]).astype(np.int32).tofile(d)

# save station data with scaling factors
with open(stat64, "w") as s:
    s.write(b64encode(stations).decode("utf-8"))

# encode scaled binary data
# TODO: use a numpy hack to do this (like writing in reverse with seek)
# or write every nth byte first with spacing or skip every 4th byte
# fmt: off
call([
    "sox",
    "-t", "raw",
    "-r", str(sample_rate),
    "-b", "32",
    "-L",
    "-e", "signed-integer",
    "-c", "3",
    raw32,
    "-t", "raw",
    "-b", "24",
    raw24,
])
# fmt: on
os.remove(raw32)

# encode metadata as tags
call(
    [
        "flac",
        "-f",
        "--tag=nstat={}".format(stations.size),
        "--tag=nt={}".format(bb.nt),
        "--tag=duration={}".format(str(bb.duration)),
        "--tag=dt={}".format(str(bb.dt)),
        "--tag=start_sec={}".format(str(bb.start_sec)),
        "--tag=lf_dir={}".format(bb.lf_dir),
        "--tag=lf_vm={}".format(bb.lf_vm),
        "--tag=hf_file={}".format(bb.hf_file),
        "--tag-from-file=stations={}".format(stat64),
        "-8",
        "--endian=little",
        "--channels=3",
        "--bps=24",
        "--sample-rate={}".format(sample_rate),
        "--sign=signed",
        raw24,
        "-o",
        args.bb_flac,
    ]
)
os.remove(raw24)
os.remove(stat64)
