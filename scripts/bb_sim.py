#!/usr/bin/env python
"""
Combines low frequency and high frequency seismograms.
"""

from argparse import ArgumentParser
import sys

from mpi4py import MPI
import numpy as np

from qcore.siteamp_models import nt2n, cb_amp
import qcore.timeseries
ampdeamp = timeseries.ampdeamp
bwfilter = timeseries.bwfilter

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()
master = 0
is_master = not rank
HEAD_SIZE = 0x0500
HEAD_STAT = 0x2c

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
#
lfdt = lf.dt
hfdt = hf.dt
lfn2 = nt2n(lf.nt)
hfn2 = nt2n(hf.nt)
lf_start_sec = -1.0
bb_start_sec = min(lf_start_sec, hf.start_sec)
ddt = int(abs(max(lf_start_sec, hf.start_sec) - bb_start_sec) / lfdt)
bbnt = max(lf.nt, hf.nt) + ddt
head_total = HEAD_SIZE + lf.stations.size * HEAD_STAT

# load velocity model
sys.path.insert(0, args.lf_vm)
import params_vel
lfvs = np.memmap('%s/vs3dfile.s' % (args.lf_vm), dtype = '<f4', \
                 shape = (int(params_vel.ny), \
                          int(params_vel.nz), \
                          int(params_vel.nx)))
# load vs30ref
vsites = dict(np.loadtxt(args.vsite_file, \
                         dtype = [('name', '|S8'), ('vs30', 'f')]))

# initialise output with general metadata
if is_master:
    with open(args.out_file, mode = 'w+b') as out:
        # save int/bool
        np.array([lf.stations.size, bbnt], dtype = 'i4').tofile(out)
        # save float parameters
        np.array([bbnt * lfdt, lfdt, bb_start_sec], \
                 dtype = 'f4').tofile(out)
        # save string parameters
        np.array([args.lf_dir, args.lf_vm, args.hf_file], \
                 dtype = '|S256').tofile(out)
        # fill space
        out.seek(head_total + lf.stations.size * bbnt * 3 * 4 - 4)
        np.zeros(1, dtype = 'i4').tofile(out)
comm.Barrier()

# load container to write to
bin_data = np.memmap(args.out_file, mode = 'r+', dtype = 'f4', \
                     shape = (lf.stations.size, bbnt, 3), offset = head_total)

my_stations = hf.stations[rank::size]
stati = rank
for stat in my_stations:
    try:
        vsite = vsites[stat.name]
    except KeyError:
        print('station not found in vs30ref: %s, using 500 cm/s.' % (stat.name))
        vsite = 500.0
    stat_lfvs = lfvs[lf.stations.y[stati]][0][lf.stations.x[stati]]
    lf_acc = np.copy(lf.acc(stat.name))
    hf_acc = np.copy(hf.acc(stat.name))
    bb_acc = np.empty(shape = (lf.nt + ddt, 3))
    pga = np.max(np.abs(hf_acc), axis = 0) / 981.0
    # not optimised or fully tested below this point
    for c in xrange(3):
        hf_acc[:, c] = ampdeamp(hf_acc[:, c], \
                             cb_amp(hfdt, hfn2, stat.vs, vsite, stat.vs, \
                                    pga[c]), amp = True)
        hf_acc[:, c] = bwfilter(hf_acc[:, c], hfdt, 1.0, 'highpass')
        lf_acc[:, c] = ampdeamp(lf_acc[:, c], \
                             cb_amp(lfdt, lfn2, stat_lfvs, vsite, stat_lfvs, \
                                    pga[c]), amp = True)
        lf_acc[:, c] = bwfilter(lf_acc[:, c], lfdt, 1.0, 'lowpass')
        bb_acc[:, c] = np.cumsum((np.hstack(([0] * ddt, hf_acc[:, c])) + \
                               np.hstack((lf_acc[:, c], [0] * ddt))) * lfdt)
    bin_data[stati] = bb_acc
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
    #bb_stations.lf_vs_ref = lf_vs_ref
    #bb_stations.vsite = vsite
    # save station info after general header
    with open(args.out_file, mode = 'r+b') as out:
        out.seek(HEAD_SIZE)
        bb_stations.tofile(out)
