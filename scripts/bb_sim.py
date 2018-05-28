#!/usr/bin/env python
"""
Combines low frequency and high frequency seismograms.
"""

from argparse import ArgumentParser
import sys

from mpi4py import MPI
import numpy as np

from qcore.siteamp_models import nt2n, cb_amp
from qcore import timeseries
ampdeamp = timeseries.ampdeamp
bwfilter = timeseries.bwfilter
acc2vel = timeseries.acc2vel
HEAD_SIZE = timeseries.BBSeis.HEAD_SIZE
HEAD_STAT = timeseries.BBSeis.HEAD_STAT

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
    arg('lf_dir', help = 'LF OutBin folder containing SEIS files')
    arg('lf_vm', help = 'LF VM folder containing velocity model')
    arg('hf_file', help = 'HF file path')
    arg('vsite_file', help = 'Vs30 station file')
    arg('out_file', help = 'BB output file path')
    try:
        args = parser.parse_args()
    except SystemExit:
        # invalid arguments or -h
        comm.Abort()
args = comm.bcast(args, root = master)

# load data stores
lf = timeseries.LFSeis(args.lf_dir)
hf = timeseries.HFSeis(args.hf_file)

# compatibility validation
# abort if behaviour is undefined
if is_master:
    if not lf.nstat == hf.nstat:
        print('LF nstat != HF nstat. %s vs %s' % (lf.nstat, hf.nstat))
        comm.Abort()
    if not np.array_equiv(lf.stations.name, hf.stations.name):
        print('LF and HF were run with different station files')
        comm.Abort()
    if not np.isclose(lf.dt * lf.nt, hf.dt * hf.nt, atol = min(lf.dt, hf.dt)):
        print('LF duration != HF duration. %s vs %s' % (lf.dt * lf.nt, \
                                                        hf.dt * hf.nt))
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

# load velocity model
sys.path.insert(0, args.lf_vm)
from params_vel import nx, ny, nz
lfvs = np.memmap('%s/vs3dfile.s' % (args.lf_vm), dtype = '<f4', \
                 shape = (int(ny), int(nz), int(nx))) \
                [lf.stations.y, 0, lf.stations.x] / 1000.0
# load vs30ref
try:
    vsites = np.vectorize(dict(np.loadtxt(args.vsite_file, \
                                          dtype = [('name', '|S8'), \
                                                   ('vs30', 'f')])) \
                          .get)(lf.stations.name)
except TypeError:
    if is_master:
        print('vsite file is missing stations')
    comm.Barrier()
    comm.Abort()

# initialise output with general metadata
if is_master:
    with open(args.out_file, mode = 'w+b') as out:
        # save int/bool
        np.array([lf.stations.size, bb_nt], dtype = 'i4').tofile(out)
        # save float parameters
        np.array([bb_nt * bb_dt, bb_dt, bb_start_sec], \
                 dtype = 'f4').tofile(out)
        # save string parameters
        np.array([args.lf_dir, args.lf_vm, args.hf_file], \
                 dtype = '|S256').tofile(out)
        # fill space
        out.seek(head_total + lf.stations.size * bb_nt * 3 * 4 - 4)
        np.zeros(1, dtype = 'i4').tofile(out)
comm.Barrier()

# load container to write to
bin_data = np.memmap(args.out_file, mode = 'r+', dtype = 'f4', \
                     shape = (lf.stations.size, bb_nt, 3), offset = head_total)

# work on station subset
my_stations = hf.stations[rank::size]
stati = rank
for stat in my_stations:
    vsite = vsites[stati]
    stat_lfvs = lfvs[stati]
    lf_acc = np.copy(lf.acc(stat.name, dt = bb_dt))
    hf_acc = np.copy(hf.acc(stat.name, dt = bb_dt))
    pga = np.max(np.abs(hf_acc), axis = 0) / 981.0
    # ideally remove loop
    for c in xrange(3):
        hf_acc[:, c] = bwfilter(ampdeamp(hf_acc[:, c], \
                                cb_amp(bb_dt, n2, stat.vs, vsite, stat.vs, \
                                pga[c]), amp = True), bb_dt, 1.0, 'highpass')
        lf_acc[:, c] = bwfilter(ampdeamp(lf_acc[:, c], \
                                cb_amp(bb_dt, n2, stat_lfvs, vsite, stat_lfvs, \
                                pga[c]), amp = True), bb_dt, 1.0, 'lowpass')
        bin_data[stati, :, c] = acc2vel((np.hstack((d_ts, hf_acc[:, c])) + \
                                         np.hstack((lf_acc[:, c], d_ts))), \
                                         bb_dt)
    # next station index
    stati += size

# combine station metadata
if is_master:
    bb_stations = np.rec.array(np.empty(lf.nstat, \
                    dtype = [('lon', 'f4'), ('lat', 'f4'), ('name', '|S8'), \
                    ('x', 'i4'), ('y', 'i4'), ('z', 'i4'), ('e_dist', 'f4'), \
                    ('hf_vs_ref', 'f4'), ('lf_vs_ref', 'f4'), ('vsite', 'f4')]))
    # copy most from LF
    for k in bb_stations.dtype.names[:-4]:
        bb_stations[k] = lf.stations[k]
    # add e_dist and hf_vs_ref from HF
    # assuming same order, true if run with same station file, asserted above
    bb_stations.e_dist = hf.stations.e_dist
    bb_stations.hf_vs_ref = hf.stations.vs
    # lf_vs_ref from velocity model
    bb_stations.lf_vs_ref = lfvs
    # vsite from vsite file
    bb_stations.vsite = vsites
    # save station info after general header
    with open(args.out_file, mode = 'r+b') as out:
        out.seek(HEAD_SIZE)
        bb_stations.tofile(out)
