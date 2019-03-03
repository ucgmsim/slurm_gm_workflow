#!/usr/bin/env python
"""
Combines low frequency and high frequency seismograms.
"""

from argparse import ArgumentParser
import os
import sys
import json

from mpi4py import MPI
import numpy as np
import json

from qcore.siteamp_models import nt2n, cb_amp
from qcore import timeseries

ampdeamp = timeseries.ampdeamp
bwfilter = timeseries.bwfilter
HEAD_SIZE = timeseries.BBSeis.HEAD_SIZE
HEAD_STAT = timeseries.BBSeis.HEAD_STAT
FLOAT_SIZE = 0x4
N_COMP = 3

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()
master = 0
is_master = not rank

# collect required arguements
args = None
if is_master:
    parser = ArgumentParser()
    arg = parser.add_argument
    arg("lf_dir", help="LF OutBin folder containing SEIS files")
    arg("lf_vm", help="LF VM folder containing velocity model")
    arg("hf_file", help="HF file path")
    arg("vsite_file", help="Vs30 station file")
    arg("out_file", help="BB output file path")
    arg("--flo", help="low/high frequency cutoff", type=float)
    arg("--fmin", help="fmin for site amplification", type=float, default=0.2)
    arg("--fmidbot", help="fmidbot for site amplification", type=float, default=0.5)
    arg("--lfvsref", help="Override LF Vs30 reference value (m/s)", type=float)
    arg(
        "--no-lf-amp",
        help="Disable site amplification for LF component",
        action="store_true",
    )

    try:
        args = parser.parse_args()
    except SystemExit:
        # invalid arguments or -h
        comm.Abort()

args = comm.bcast(args, root=master)
if args.no_lf_amp:

    def ampdeamp_lf(series, *x, **y):
        return series

    def cb_amp_lf(*x, **y):
        pass


else:
    ampdeamp_lf = ampdeamp
    cb_amp_lf = cb_amp

# load data stores
lf = timeseries.LFSeis(args.lf_dir)
hf = timeseries.HFSeis(args.hf_file)

# compatibility validation
# abort if behaviour is undefined
if is_master:
    if not lf.nstat == hf.nstat:
        print("LF nstat != HF nstat. %s vs %s" % (lf.nstat, hf.nstat))
        comm.Abort()
    if not np.array_equiv(lf.stations.name, hf.stations.name):
        print("LF and HF were run with different station files")
        comm.Abort()
    if not np.isclose(lf.dt * lf.nt, hf.dt * hf.nt, atol=min(lf.dt, hf.dt)):
        print("LF duration != HF duration. %s vs %s" % (lf.dt * lf.nt, hf.dt * hf.nt))
        comm.Abort()

# load metadata
lf_start_sec = -1.0
bb_start_sec = min(lf_start_sec, hf.start_sec)
bb_dt = min(lf.dt, hf.dt)
d_nt = int(round(max(lf_start_sec, hf.start_sec) - bb_start_sec) / bb_dt)
bb_nt = int(round(max(lf.duration, hf.duration) / bb_dt + d_nt))
n2 = nt2n(bb_nt)
d_ts = np.zeros(d_nt)
head_total = HEAD_SIZE + lf.stations.size * HEAD_STAT
file_size = head_total + lf.stations.size * bb_nt * N_COMP * FLOAT_SIZE
if args.flo is None:
    # min_vs / (5.0 * hh)
    args.flo = 0.5 / (5.0 * lf.hh)

# load vs30ref
if args.lfvsref is None:
    # vs30ref from velocity model
    with open("%s/params_vel.json" % (args.lf_vm), "r") as j:
        vm_conf = json.load(j)
    lfvs30refs = (
        np.memmap(
            "%s/vs3dfile.s" % (args.lf_vm),
            dtype="<f4",
            shape=(vm_conf["ny"], vm_conf["nz"], vm_conf["nx"]),
            mode="r",
        )[lf.stations.y, 0, lf.stations.x]
        * 1000.0
    )
else:
    # fixed vs30ref
    lfvs30refs = np.ones(lf.stations.size, dtype=np.float32) * args.lfvsref

# load vs30
try:
    # has to be a numpy array of np.float32 as written directly to binary
    vs30s = np.vectorize(
        dict(
            np.loadtxt(
                args.vsite_file,
                dtype=[("name", "U7"), ("vs30", "f4")],
                comments=("#", "%"),
            )
        ).get
    )(lf.stations.name)
    assert not np.isnan(vs30s).any()
except AssertionError:
    if is_master:
        print("vsite file is missing stations")
        comm.Abort()


# initialise output with general metadata
def initialise(check_only=False):
    with open(args.out_file, mode="rb" if check_only else "w+b") as out:
        # int/bool parameters
        i = np.array([lf.stations.size, bb_nt], dtype="i4")
        # float parameters
        f = np.array([bb_nt * bb_dt, bb_dt, bb_start_sec], dtype="f4")
        # string parameters
        s = np.array([args.lf_dir, args.lf_vm, args.hf_file], dtype="|S256")
        # station metadata
        bb_stations = np.rec.array(
            np.zeros(
                lf.nstat,
                dtype={
                    "names": [
                        "lon",
                        "lat",
                        "name",
                        "x",
                        "y",
                        "z",
                        "e_dist",
                        "hf_vs_ref",
                        "lf_vs_ref",
                    ],
                    "formats": ["f4", "f4", "|S8", "i4", "i4", "i4", "f4", "f4", "f4"],
                    "itemsize": HEAD_STAT,
                },
            )
        )
        # copy most from LF
        for col in bb_stations.dtype.names[:-3]:
            bb_stations[col] = lf.stations[col]
        # add e_dist and hf_vs_ref from HF
        # assuming same order, true if run with same station file, asserted above
        bb_stations.e_dist = hf.stations.e_dist
        bb_stations.hf_vs_ref = hf.stations.vs
        # lf_vs_ref from velocity model
        bb_stations.lf_vs_ref = lfvs30refs

        # verify or write
        if check_only:
            for a in [i, f, s, bb_stations]:
                if a is bb_stations:
                    out.seek(HEAD_SIZE)
                assert np.min(np.fromfile(out, dtype=a.dtype, count=a.size) == a)
        else:
            i.tofile(out)
            f.tofile(out)
            s.tofile(out)
            out.seek(HEAD_SIZE)
            bb_stations.tofile(out)
            # fill space
            out.seek(file_size - FLOAT_SIZE)
            np.float32().tofile(out)


def unfinished():
    try:
        with open(args.out_file, "rb") as bbf:
            bbf.seek(HEAD_SIZE)
            # checkpoints are vsite written to file
            # assume continuing machine is the same endian
            ckpoints = (
                np.fromfile(
                    bbf,
                    count=lf.stations.size,
                    dtype={
                        "names": ["vsite"],
                        "formats": ["f4"],
                        "offsets": [40],
                        "itemsize": HEAD_STAT,
                    },
                )["vsite"]
                > 0
            )
    except IOError:
        # file not created yet
        return
    if os.stat(args.out_file).st_size != file_size:
        # file size is incorrect (probably different simulation)
        return
    if np.min(ckpoints):
        try:
            initialise(check_only=True)
            print("BB Simulation already completed.")
            comm.Abort()
        except AssertionError:
            return
    # seems ok to continue simulation
    return np.invert(ckpoints)


station_mask = None
if is_master:
    station_mask = unfinished()
    if station_mask is None or sum(station_mask) == lf.stations.size:
        print("No valid checkpoints found. Starting fresh simulation.")
        initialise()
        station_mask = np.ones(lf.stations.size, dtype=np.bool)
    else:
        try:
            initialise(check_only=True)
            print(
                "%d of %d stations completed. Resuming simulation."
                % (lf.stations.size - sum(station_mask), lf.stations.size)
            )
        except AssertionError:
            print("Simulation parameters mismatch. Starting fresh simulation.")
            initialise()
            station_mask = np.ones(lf.stations.size, dtype=np.bool)
station_mask = comm.bcast(station_mask, root=master)
stations_todo = hf.stations[station_mask][rank::size]
stations_todo_idx = np.arange(hf.stations.size)[station_mask][rank::size]

# load container to write to
bin_data = open(args.out_file, "r+b")
bin_seek = head_total + stations_todo_idx * bb_nt * N_COMP * FLOAT_SIZE
bin_seek_vsite = HEAD_SIZE + stations_todo_idx * HEAD_STAT + 40

# work on station subset
fmin = args.fmin
fmidbot = args.fmidbot
t0 = MPI.Wtime()
bb_acc = np.empty((bb_nt, N_COMP), dtype="f4")
for i, stat in enumerate(stations_todo):
    vs30 = vs30s[stations_todo_idx[i]]
    lfvs30ref = lfvs30refs[stations_todo_idx[i]]
    lf_acc = np.copy(lf.acc(stat.name, dt=bb_dt))
    hf_acc = np.copy(hf.acc(stat.name, dt=bb_dt))
    pga = np.max(np.abs(hf_acc), axis=0) / 981.0
    # ideally remove loop
    for c in range(3):
        hf_acc[:, c] = bwfilter(
            ampdeamp(
                hf_acc[:, c],
                cb_amp(
                    bb_dt,
                    n2,
                    stat.vs,
                    vs30,
                    stat.vs,
                    pga[c],
                    fmin=fmin,
                    fmidbot=fmidbot,
                ),
                amp=True,
            ),
            bb_dt,
            args.flo,
            "highpass",
        )
        lf_acc[:, c] = bwfilter(
            ampdeamp_lf(
                lf_acc[:, c],
                cb_amp_lf(
                    bb_dt,
                    n2,
                    lfvs30ref,
                    vs30,
                    stat.vs,
                    pga[c],
                    fmin=fmin,
                    fmidbot=fmidbot,
                ),
                amp=True,
            ),
            bb_dt,
            args.flo,
            "lowpass",
        )
        bb_acc[:, c] = (
            np.hstack((d_ts, hf_acc[:, c])) + np.hstack((lf_acc[:, c], d_ts))
        ) / 981.0
    bin_data.seek(bin_seek[i])
    bb_acc.tofile(bin_data)
    # write vsite as used for checkpointing
    bin_data.seek(bin_seek_vsite[i])
    vs30.tofile(bin_data)
bin_data.close()
print("Process %03d of %03d finished (%.2fs)." % (rank, size, MPI.Wtime() - t0))
