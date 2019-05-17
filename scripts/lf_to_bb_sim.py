#!/usr/bin/env python
"""
Combines low frequency and high frequency seismograms.
"""

from argparse import ArgumentParser
import os
import logging
from mpi4py import MPI
import numpy as np

from qcore.siteamp_models import nt2n, cb_amp
from qcore import MPIFileHandler, timeseries, utils
from qcore.constants import VM_PARAMS_FILE_NAME

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

logger = logging.getLogger("rank_%i" % comm.rank)
logger.setLevel(logging.DEBUG)

# collect required arguments
args = None
if is_master:
    parser = ArgumentParser()
    arg = parser.add_argument
    arg("lf_dir", help="LF OutBin folder containing SEIS files")
    arg("lf_vm", help="LF VM folder containing velocity model")
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
        args = parser.parse_known_args()[0]
    except SystemExit:
        # invalid arguments or -h
        comm.Abort()

args = comm.bcast(args, root=master)

mh = MPIFileHandler.MPIFileHandler(
    os.path.join(os.path.dirname(args.out_file), "BB.log")
)
formatter = logging.Formatter("%(asctime)s:%(name)s:%(levelname)s:%(message)s")
mh.setFormatter(formatter)
logger.addHandler(mh)


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

# load metadata
lf_start_sec = -1.0
bb_start_sec = lf_start_sec
bb_dt = lf.dt
d_nt = int(round(lf_start_sec - bb_start_sec) / bb_dt)
bb_nt = int(round(lf.duration / bb_dt + d_nt))
n2 = nt2n(bb_nt)
d_ts = np.zeros(d_nt)
head_total = HEAD_SIZE + lf.stations.size * HEAD_STAT
file_size = head_total + lf.stations.size * bb_nt * N_COMP * FLOAT_SIZE
if args.flo is None:
    # min_vs / (5.0 * hh)
    args.flo = 0.5 / (5.0 * lf.hh)

if is_master:
    # Logging each argument
    for key in vars(args):
        logger.debug("{} : {}".format(key, getattr(args, key)))

comm.Barrier()  # prevent other processes from messing log file until master is done with logging above
# load vs30ref
if args.lfvsref is None:
    # vs30ref from velocity model
    vm_conf = utils.load_yaml(os.path.join(args.lf_vm, VM_PARAMS_FILE_NAME))
    lfvs30refs = (
        np.memmap(
            "%s/vs3dfile.s" % (args.lf_vm),
            dtype="<f4",
            shape=(vm_conf["ny"], vm_conf["nz"], vm_conf["nx"]),
            mode="r",
        )[lf.stations.y, 0, lf.stations.x]
        * 1000.0
    )
    if is_master:
        logger.debug("vs30ref from velocity model.")
else:
    # fixed vs30ref
    lfvs30refs = np.ones(lf.stations.size, dtype=np.float32) * args.lfvsref
    if is_master:
        logger.debug("fixed vs30ref.")

# load vs30
try:
    # has to be a numpy array of np.float32 as written directly to binary
    vs30s = np.vectorize(
        dict(
            np.loadtxt(
                args.vsite_file,
                dtype=[("name", "|U8"), ("vs30", "f4")],
                comments=("#", "%"),
            )
        ).get
    )(lf.stations.name)
    assert not np.isnan(vs30s).any()
except AssertionError:
    if is_master:
        logger.error("vsite file is missing stations.")
        comm.Abort()
else:
    if is_master:
        logger.debug("vs30 loaded successfully.")


# initialise output with general metadata
def initialise(check_only=False):
    logger.debug("Initialising.")
    with open(args.out_file, mode="rb" if check_only else "w+b") as out:
        # int/bool parameters
        i = np.array([lf.stations.size, bb_nt], dtype="i4")
        # float parameters
        f = np.array([bb_nt * bb_dt, bb_dt, bb_start_sec], dtype="f4")
        # string parameters
        s = np.array([args.lf_dir, args.lf_vm], dtype="|S256")
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
                        "lf_vs_ref",
                    ],
                    "formats": ["f4", "f4", "|S8", "i4", "i4", "i4", "f4", "f4"],
                    "itemsize": HEAD_STAT,
                },
            )
        )
        # copy most from LF
        for col in bb_stations.dtype.names[:-3]:
            bb_stations[col] = lf.stations[col]
        # assuming same order, true if run with same station file, asserted above
        bb_stations.e_dist = lf.stations.e_dist
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
            logger.debug("Checkpoints found.")
            initialise(check_only=True)
            logger.error("BB Simulation already completed.")
            comm.Abort()
        except AssertionError:
            return
    # seems ok to continue simulation
    return np.invert(ckpoints)


t0 = MPI.Wtime()

print("Process %03d of %03d finished (%.2fs)." % (rank, size, MPI.Wtime() - t0))
comm.Barrier()  # all ranks wait here until rank 0 arrives to announce all completed
if is_master:
    logger.debug("Simulation completed.")
