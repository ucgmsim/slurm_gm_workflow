#!/usr/bin/env python
"""
Combines low frequency and high frequency seismograms.
"""

from argparse import ArgumentParser
import sys
sys.path.insert(0, '/home/vap30/ucgmsim/qcore/qcore/')

from mpi4py import MPI
import numpy as np

from timeseries import LFSeis, HFSeis, BBSeis, vel2acc, acc2vel

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
    arg('hf_file', help = 'HF file path')
    arg('out_file', help = 'BB output file path')
    try:
        args = parser.parse_args()
    except SystemExit:
        # invalid arguments or -h
        comm.Abort()
args = comm.bcast(args, root = master)

# load data stores
lf = LFSeis(args.lf_dir)
hf = HFSeis(args.hf_file)

# compatibility validation
# abort if behaviour is undefined
if is_master:
    if not lf.nstat == hf.nstat:
        print('LF nstat != HF nstat. %s vs %s' % (lf.nstat, hf.nstat))
        comm.Abort()
    if not np.array_equiv(lf.stations.name, hf.stations.name):
        print('LF and HF were run with different station files')
        comm.Abort()
    if not lf.dt == hf.dt:
        print('LF dt != HF dt. %s vs %s' % (lf.dt, hf.dt))
        comm.Abort()
    if not lf.nt == hf.nt:
        print('LF nt != HF nt. %s vs %s' % (lf.nt, hf.nt))
        comm.Abort()

my_stations = hf.stations.name[rank::size]
print my_stations
for stat in my_stations:
    lf_acc = lf.acc(stat)
    hf_acc = hf.acc(stat)

# combine station metadata
if is_master:
    bb_stations = np.rec.array(np.empty(lf.nstat, \
                    dtype = [('lon', 'f4'), ('lat', 'f4'), ('name', '|S8'), \
                    ('x', 'i4'), ('y', 'i4'), ('z', 'i4'), ('e_dist', 'f4')]))
    # copy most from LF
    for k in bb_stations.dtype.names[:-1]:
        bb_stations[k] = lf.stations[k]
    # add e_dist from HF
    # assuming same order, true if run with same station file, asserted above
    bb_stations.e_dist = hf.stations.e_dist
