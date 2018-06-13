#!/usr/bin/env python
"""
Simulates high frequency seismograms for stations.
"""
from argparse import ArgumentParser
import math
import os
from subprocess import call, Popen, PIPE
import sys
from tempfile import mkstemp

from mpi4py import MPI
import numpy as np

from qcore.config import qconfig

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()
master = 0
is_master = not rank
HEAD_SIZE = 0x0200
HEAD_STAT = 0x18
MAX_STATLIST = 1000

# never changed / unknown function (line 6)
nbu = 4
ift = 0
flo = 0.02
fhi = 19.9
# for line 15
nl_skip = -99
vp_sig = 0.0
vsh_sig = 0.0
rho_sig = 0.0
qs_sig = 0.0
ic_flag = True
# seems to store details in {velocity_name}_{station_name}.1d if not '-1'
velocity_name = '-1'

args = None
if is_master:
    parser = ArgumentParser()
    arg = parser.add_argument
    # HF IN, line 12
    arg('stoch_file', help = 'rupture model')
    # HF IN, line 2
    arg('station_file', help = 'station file (lon, lat, name)')
    # HF IN, line 3
    arg('out_file', help = 'file path for HF output')
    # ARG 0
    arg('--sim_bin', help = 'high frequency binary (modified for binary out)', \
        default = os.path.join(qconfig['tools_dir'], 'hb_high_v5.4.5_binmod'))
    arg('--t-sec', help = 'high frequency output start time', \
        type = float, default = 0.0)
    # HF IN, line 1
    arg('--sdrop', help = 'stress drop average (bars)', \
        type = float, default = 50.0)
    # HF IN, line 4
    arg('--rayset', help = 'ray types 1:direct 2:moho', nargs = '+', \
        type = int, default = [1, 2])
    # HF IN, line 5
    arg('--no-siteamp', help = 'disable BJ97 site amplification factors', \
        action = 'store_true')
    # HF IN, line 7
    arg('--seed', help = 'random seed', type = int, default = 5481190)
    arg('-i', '--independent', action = 'store_true', \
        help = 'run stations independently (with same random seed)')
    # HF IN, line 9
    arg('--duration', help = 'output length (seconds)', \
        type = float, default = 100.0)
    arg('--dt', help = 'timestep (seconds)', type = float, default = 0.005)
    arg('--fmax', help = 'max sim frequency (Hz)', type = float, default = 10)
    arg('--kappa', help = '', type = float, default = 0.045)
    arg('--qfexp', help = 'Q frequency exponent', type = float, default = 0.6)
    # HF IN, line 10
    arg('--rvfac', help = 'rupture velocity factor (rupture : Vs)', \
        type = float, default = 0.8)
    arg('--rvfac_shal', help = 'rvfac shallow fault multiplier', \
        type = float, default = 0.7)
    arg('--rvfac_deep', help = 'rvfac deep fault multiplier', \
        type = float, default = 0.7)
    arg('--czero', help = 'C0 coefficient, < -1 for binary default', \
        type = float, default = 2.1)
    arg('--calpha', help = 'Ca coefficient, < -1 for binary default', \
        type = float, default = -99)
    # HF IN, line 11
    arg('--mom', help = 'seismic moment, -1: use rupture model', \
        type = float, default = -1.0)
    arg('--rupv', help = 'rupture velocity, -1: use rupture model', \
        type = float, default = -1.0)
    # HF IN, line 13
    arg('-m', '--velocity-model', \
        help = 'path to velocity model (1D)', \
        default = os.path.join(qconfig['VEL_MOD'], \
                               'Mod-1D/Cant1D_v2-midQ_leer.1d'))
    arg('-s', '--site-vm-dir', \
        help = 'dir containing site specific velocity models (1D)')
    # HF IN, line 14
    arg('--vs-moho', help = 'depth to moho, < 0 for 999.9', \
        type = float, default = 999.9)
    # HF IN, line 17
    arg('--fa_sig1', help = 'fourier amplitute uncertainty (1)', \
        type = float, default = 0.0)
    arg('--fa_sig2', help = 'fourier amplitude uncertainty (2)', \
        type = float, default = 0.0)
    arg('--rv_sig1', help = 'rupture velocity uncertainty', \
        type = float, default = 0.1)
    # HF IN, line 18
    arg('--path-dur', help = '''path duration model
        0:GP2010 formulation
        1:[DEFAULT] WUS modification trial/error
        2:ENA modification trial/error
        11:WUS formulation of BT2014, overpredicts for multiple rays
        12:ENA formulation of BT2015, overpredicts for multiple rays''', \
        type = int, default = 1)
    try:
        args = parser.parse_args()
    except SystemExit:
        # invalid arguments or -h
        comm.Abort()
args = comm.bcast(args, root = master)
nt = int(args.duration / args.dt)
stations = np.loadtxt(args.station_file, \
                      dtype = [('lon', 'f4'), ('lat', 'f4'), ('name', '|S8')])
head_total = HEAD_SIZE + HEAD_STAT * stations.shape[0]

# initialise output with general metadata
if is_master:
    with open(args.out_file, mode = 'w+b') as out:
        # save int/bool parameters, rayset must be fixed to length = 4
        fwrs = args.rayset + [0] * (4 - len(args.rayset))
        np.array([stations.shape[0], nt, args.seed, not args.no_siteamp, \
                  args.path_dur, len(args.rayset), \
                  fwrs[0], fwrs[1], fwrs[2], fwrs[3], \
                  nbu, ift, nl_skip, ic_flag, args.independent, \
                  args.site_vm_dir != None], dtype = 'i4').tofile(out)
        # save float parameters
        np.array([args.duration, args.dt, args.t_sec, args.sdrop, args.kappa, \
                  args.qfexp, args.fmax, flo, fhi, \
                  args.rvfac, args.rvfac_shal, args.rvfac_deep, \
                  args.czero, args.calpha, args.mom, args.rupv, args.vs_moho, \
                  vp_sig, vsh_sig, rho_sig, qs_sig, \
                  args.fa_sig1, args.fa_sig2, args.rv_sig1], \
                 dtype = 'f4').tofile(out)
        # save string parameters
        if args.site_vm_dir != None:
            vm = args.site_vm_dir
        else:
            vm = args.velocity_model
        np.array(map(os.path.basename, [args.stoch_file, vm]), \
                dtype = '|S64').tofile(out)
comm.Barrier()

def run_hf(local_statfile, n_stat, idx_0, velocity_model = args.velocity_model):
    """
    Runs HF Fortran code.
    """
    stdin = '\n'.join(['', str(args.sdrop), local_statfile, args.out_file, \
        '%d %s' % (len(args.rayset), ' '.join(map(str, args.rayset))), \
        str(int(not args.no_siteamp)), \
        '%d %d %s %s' % (nbu, ift, flo, fhi), \
        str(args.seed), str(n_stat), \
        '%s %s %s %s %s' \
            % (args.duration, args.dt, args.fmax, args.kappa, args.qfexp), \
        '%s %s %s %s %s' % (args.rvfac, args.rvfac_shal, args.rvfac_deep, \
                            args.czero, args.calpha), \
        '%s %s' % (args.mom, args.rupv), \
        args.stoch_file, args.velocity_model, str(args.vs_moho), \
        '%d %s %s %s %s %d' \
            % (nl_skip, vp_sig, vsh_sig, rho_sig, qs_sig, ic_flag), \
        velocity_name, \
        '%s %s %s' % (args.fa_sig1, args.fa_sig2, args.rv_sig1), \
        str(args.path_dur), str(head_total + idx_0 * (nt * 3 * 4)), ''])

    # run HF binary
    p = Popen([args.sim_bin], stdin = PIPE, stderr = PIPE)
    stderr = p.communicate(stdin)[1]
    # load vs
    with open(velocity_model, 'r') as vm:
        vm.readline()
        vs = float(vm.readline().split()[2]) * 1000.0
    p.wait()
    # edist is the only other variable that HF calculates
    e_dist = np.fromstring(stderr, dtype = 'f4', sep = '\n')
    assert(e_dist.size == n_stat)
    return e_dist, vs

# distribute work, must be sequential segments for processes
d = stations.shape[0] // size
r = stations.shape[0] % size
start = rank * d + min(r, rank)
work = stations[start:start + d + (rank < r)]
max_nstat = int(math.ceil(stations.shape[0] / float(size)))

# process data to give Fortran code
e_dist = np.empty(max_nstat, dtype = 'f4') * np.nan
vs = np.empty(max_nstat, dtype = 'f4') * np.nan
in_stats = mkstemp()[1]
if args.independent:
    vm = args.velocity_model
    for s in xrange(work.shape[0]):
        if args.site_vm_dir != None:
            vm = os.path.join(args.site_vm_dir, '%s.1d' % (stations[s]['name']))
        np.savetxt(in_stats, work[s:s + 1], fmt = '%f %f %s')
        e_dist[s], vs[s] = run_hf(in_stats, 1, start + s, velocity_model = vm)
else:
    for s in xrange(0, work.shape[0], MAX_STATLIST):
        n_stat = min(MAX_STATLIST, work.shape[0] - s)
        sidx = slice(s, s + n_stat)
        np.savetxt(in_stats, work[sidx], fmt = '%f %f %s')
        e_dist[sidx], vs[sidx] = run_hf(in_stats, n_stat, start + s)
os.remove(in_stats)

# gather station metadata
recvbuf = None
if is_master:
    recvbuf = np.empty([size, max_nstat], dtype='f4')
comm.Gather(e_dist, recvbuf, root = master)
if is_master:
    e_dist = recvbuf[np.isfinite(recvbuf)]
comm.Gather(vs, recvbuf, root = master)
if is_master:
    vs = recvbuf[np.isfinite(recvbuf)]
    # add station metadata to output
    stat_head = np.zeros(stations.shape, dtype = np.dtype(stations.dtype.descr \
            + [('e_dist', 'f4'), ('vs', 'f4')]))
    stat_head['lon'] = stations['lon']
    stat_head['lat'] = stations['lat']
    stat_head['name'] = stations['name']
    stat_head['e_dist'] = e_dist
    stat_head['vs'] = vs
    # save station info after general header
    with open(args.out_file, mode = 'r+b') as out:
        out.seek(HEAD_SIZE)
        stat_head.tofile(out)
