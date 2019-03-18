#!/usr/bin/env python
"""
Reduce stations in BBSeis file.
"""

from argparse import ArgumentParser
import os
import sys

import numpy as np

from qcore.timeseries import BBSeis

parser = ArgumentParser()
parser.add_argument("bb1_bin", help="path to original binary broadband file")
parser.add_argument("bb2_bin", help="path to new binary broadband file")
parser.add_argument("ll_file", help="station file")
parser.add_argument(
    "--ll_col", help="station file station name column index", type=int, default=2
)
args = parser.parse_args()
assert os.path.isfile(args.bb1_bin)
assert os.path.isfile(args.ll_file)

bb = BBSeis(args.bb1_bin)

# intersection of wanted stations (ll file) and what is available (bb file)
new_stations = np.loadtxt(args.ll_file, usecols=args.ll_col, dtype=np.unicode_, ndmin=1)
new_stations_exist = np.isin(new_stations, bb.stations.name)
print("given stations:", new_stations.size, "valid stations:", sum(new_stations_exist))
new_stations = new_stations[new_stations_exist]
if new_stations.size == 0:
    sys.exit("no stations found from ll file")

# reformat bb stations as np.string_
old_stations_dtype = bb.stations.dtype.descr
old_stations_dtype[2] = old_stations_dtype[2][0], "|S8"
old_stations = np.rec.fromrecords(bb.stations, dtype=old_stations_dtype)

with open(args.bb2_bin, "wb") as bb_new:
    ###
    ### create new header
    ###

    # copying individual items is endian safe but not format change safe
    np.array([new_stations.size, bb.nt], dtype="i4").tofile(bb_new)
    np.array([bb.duration, bb.dt, bb.start_sec], dtype="f4").tofile(bb_new)
    np.array([bb.lf_dir, bb.lf_vm, bb.hf_file], dtype="|S256").tofile(bb_new)

    ###
    ### create new station list
    ###
    bb_new.seek(1280)
    for s in new_stations:
        old_stations[bb.stat_idx[s]].tofile(bb_new)

    ###
    ### create new content
    ###
    for s in new_stations:
        bb.acc(s).tofile(bb_new)

