#!/usr/bin/env python2
"""
Simulates high frequency seismograms for stations.
"""
from argparse import ArgumentParser
import math
import os
from subprocess import call, Popen, PIPE
import sys

from mpi4py import MPI

from qcore.config import qconfig

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()
master = 0
is_master = not rank

args = None
if is_master:
    parser = ArgumentParser()
    parser.add_argument('in_file', help = 'station file (lon, lat, name)')
    parser.add_argument('out_file', help = 'file path for HF output')
    parser.add_argument('-i', '--independent', action = 'store_true', \
            help = 'run stations independently (with same random seed)')
    parser.add_argument('-m', '--velocity-model', \
            help = 'path to velocity model (1D)', \
            default = os.path.join(qconfig['VEL_MOD'], \
                                   'Mod-1D/Cant1D_v2-midQ_leer.1d'))
    parser.add_argument('-s', '--site-vm-dir', \
            help = 'dir containing site specific velocity models (1D)')
    try:
        args = parser.parse_args()
    except SystemExit:
        # invalid arguments or -h
        comm.Abort()
args = comm.bcast(args, root = master)

print args
sys.exit()
rand_reset = False
site_specific = False

hf_sim_dir = sys.argv[1]

local_statfile = os.path.join(hf_sim_dir, 'local.statfile')

sys.path.append(hf_sim_dir)

verify_strings([hf_prefix, hf_t_len, hf_dt, hf_vs_moho, hf_fa_sig_1, hf_rv_sig_1, \
                hf_sdrop, hf_kappa, hf_qfexp, hf_rayset, hf_rvfac, hf_shal_rvfac, hf_deep_rvfac, \
                hf_czero, hf_site_amp, hf_mom, hf_rupv, hf_seed, hf_fmax, hf_calpha, hf_path_dur])
stat_estimate = len(all_stations)


    statfile_base = os.path.join(hf_sim_dir, 'local.tmp.sf_')
    if rand_reset:
        jobs = stat_estimate  # we have as many jobs as the number of stations ie. each station name will be kept in one job file (ie. statfile)
    else:
        jobs = size  # each job will be taking care of stat_estimate / jobs.
    statfiles = ['%s%d' % (statfile_base, p) for p in xrange(jobs)]

out_prefix = os.path.join(hf_accdir, hf_prefix)

if rank == 0:

    # copy line not starting with '#' into a local FD_STATLIST copy
    # file pointers for each processes' FD_STATLIST portion
    # number of stations for each process
    nss = [0] * jobs
    stats_per_job = int(
        math.ceil(stat_estimate / float(jobs)))  # this will be 1 if rand_reset is True as job=stat_estimate above
    # ceiling may cause stats_per_job* jobs >= stat_estimate. (eg. 13/4 yielding 4). This may cause IndexError (handled below), but ensures all stations are correctly processed.

    for i, f in enumerate(statfiles):
        with open(f, 'w') as fp:
            for j in range(stats_per_job):
                try:
                    fp.write(all_stations[i * stats_per_job + j])
                except IndexError:
                    break  # i*stats_per_job+j may exceed "stat_estimate". Break for-j here and let for-i continue
                else:
                    nss[i] += 1
            fp.write('\n')

    # some info echoed in original script, might be useful for some
    if not site_specific:
        print("%d %s" % (sum(nss), hf_v_model))


# def run_hf(rank,local_statfile, n_stat):
def run_hf((local_statfile, n_stat)):
    global hf_v_model
    print "Rank %d Processing %s containing %d stations" % (rank, local_statfile, n_stat)

    # just in case more processes than stations
    if n_stat == 0:
        return

    # if site_specific=True, hf_v_model needs to be worked out
    if site_specific:
        #        if rank == 0 :
        #            print "Site specific 1D profile"
        if n_stat > 1:
            print "Error: For site specific, n_stat = 1 must be met. Check %s" % local_statfile
            comm.Abort()
        with open(local_statfile, 'r') as fr:
            lines = fr.readlines()  # this must be non-empty. n_stat > 0 as handled above

        stat = lines[0]
        statname = stat.split(' ')[-1].strip('\n')
        hf_v_model = os.path.join(hf_v_model_path, '%s.1d' % statname)

    #    else:
    #        if rank == 0:
    #            print "Universal 1D profile"

    cmd = hf_sim_bin
    args = '\n%s\n%s\n%s\n%s\n%s\n4   0  0.02  19.9\n%s\n%s\n%s %s %s %s %s\n%s %s %s %s %s\n%s %s\n%s\n%s\n%s\n-99 0.0 0.0 0.0 0.0 1\n-1\n%s 0.0 %s\n%s\n' % (
    hf_sdrop, local_statfile, out_prefix, hf_rayset, hf_site_amp, hf_seed, n_stat, hf_t_len, hf_dt, hf_fmax, hf_kappa,
    hf_qfexp, hf_rvfac, hf_shal_rvfac, hf_deep_rvfac, hf_czero, hf_calpha, hf_mom, hf_rupv, hf_slip, hf_v_model,
    hf_vs_moho, hf_fa_sig_1, hf_rv_sig_1, hf_path_dur)

    # prevent terminal echo slow down, not useful with many processes anyway
    #    sink = open('/dev/null', 'w')
    # sink = sys.stdout
    sink = open(os.path.join(hf_sim_dir, '%s.out' % local_statfile), 'w')
    sink.write("%s %s\n" % (cmd, args))
    sink.write("1D profile used: %s\n" % hf_v_model)

    # execute calculation, stdin from pipe
    hf_pipe = Popen([cmd], stdin=PIPE, stdout=sink)
    # pipe input data into binary stdin
    # look at hf_sim_bin source code (same dir) for more info on parameters
    hf_pipe.communicate(args)
    sink.close()


# p = Pool(size)
# p.map(run_hf, zip(statfiles, nss))
if rank == 0:
    stat_data = zip(statfiles, nss)
else:
    stat_data = [None, 0]

# statfile, nss = comm.scatter(stat_data, root=0) #all processes will be waiting for rank 0 to scatter data. No barrier needed

comm.Barrier()

if rand_reset:
    executor = parallel_executor.ParallelExecutor()
    executor.process_function_with_result(run_hf, stat_data)
else:
    statfile, nss = comm.scatter(stat_data, root=0)
    print "rank %d" % rank
    run_hf((statfile, nss))
