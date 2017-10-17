#!/usr/bin/env python2
"""
Simulates high frequency seismograms for stations.

USAGE: edit params.py as needed. Then, execute this file.
For parameter documentation, see params.py or the simulator source code.

Original file from Rob (Jan 2015).
Converted to python.
@date 05 May 2016
@author Viktor Polak
@contact viktor.polak@canterbury.ac.nz
"""
import sys
import os
from mpi4py import MPI
import math

sys.path.append(os.path.abspath(os.path.curdir))
from subprocess import call, Popen, PIPE

from shared import *
from params import *
from params_base_bb import *

import glob

# TODO: add qcore path in a better way
sys.path.append('/projects/nesi00213/qcore')
import parallel_executor
import remaining_stations


comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()


rand_reset = False
site_specific = False
option = 0
if len(sys.argv)<3:
    if rank == 0:
        print "Usage: %s hf_sim_dir [0-2] " %sys.argv[0]
        print "- hf_sim_dir is where the output will be stored"
        print "- Option:"
        print "        0 (default): may produce different result for different num procs."
        print "        1: rand_reset, enforce random number sequence reset for each station"
        print "        2: site_specific, seperate 1d profile for each station"
    comm.Abort(0)
if len(sys.argv)>=3:
    option = int(sys.argv[2])

if option >= 1:
    rand_reset = True
if option >= 2:
    site_specific = True

#if rand_reset == False and site_specific == True:
#    if rank == 0:
#        print "warning: To compute site_specific HF, rand_reset  MUST be True. Auto-setting rand_reset = True"
#    rand_reset = True

if rank == 0:
    print "Rand Reset:%s" %rand_reset
    print "Site Specific:%s" %site_specific
hf_sim_dir = sys.argv[1]

local_statfile = os.path.join(hf_sim_dir, 'local.statfile')

sys.path.append(hf_sim_dir)

from params_bb_uncertain import *
try:
    from params_bb_uncertain import hf_kappa, hf_sdrop
except ImportError:
    if rank == 0:
        print "Note: hf_kappa and hf_sdrop appear to be in params.py - old style"
    else:
        pass
finally:
    if rank == 0: 
        print "hf_kappa: %s" %hf_kappa
        print "hf_sdrop: %s" %hf_sdrop
    else:
        pass

try:
    from params_base_bb import hf_v_model
except ImportError:
    if rank == 0:
        if not site_specific:
            print "Error: hf_v_model should be in params_base_bb for non site_specific computation"
            print "Run install_bb.py again"
            comm.Abort()


if rank ==0:
    # verify input incl. params.py
    verify_binaries([hf_sim_bin])
    if site_specific:
        verify_user_dirs([hf_v_model_path])
        verify_files([FD_STATLIST,hf_slip])
        hf_v_model = None

    else:
        verify_files([FD_STATLIST, hf_slip, hf_v_model])
        verify_logfiles([local_statfile])

    verify_strings([hf_prefix, hf_t_len, hf_dt, hf_vs_moho, hf_fa_sig_1, hf_rv_sig_1, \
        hf_sdrop, hf_kappa, hf_qfexp, hf_rayset, hf_rvfac, hf_shal_rvfac, hf_deep_rvfac, \
        hf_czero, hf_site_amp, hf_mom, hf_rupv, hf_seed, hf_fmax, hf_calpha, hf_path_dur])
    verify_user_dirs([hf_sim_dir])
    


    try:
        verify_strings([hf_resume])
    except:
        hf_resume = False


    verify_user_dirs([hf_accdir],reset=not hf_resume) #to resume from the checkpoint, reset=False. The combo of hf_resume=True and rand_reset=False is not recommended for reproducibility

    all_stations = remaining_stations.get_lines(FD_STATLIST,hf_accdir,size)

    # don't give a single process too many stations to work on
    #with open(FD_STATLIST, 'r') as fp:
    #    stat_estimate = len(fp.readlines())

    stat_estimate = len(all_stations)

    if stat_estimate == 0:
        print "----------- All completed"
        comm.Abort(0)
 
    statfile_base = os.path.join(hf_sim_dir, 'local.tmp.sf_')
    if rand_reset:
        jobs = stat_estimate #we have as many jobs as the number of stations ie. each station name will be kept in one job file (ie. statfile)
    else:
        jobs = size # each job will be taking care of stat_estimate / jobs. 
    statfiles = ['%s%d' % (statfile_base, p) for p in xrange(jobs)]

out_prefix = os.path.join(hf_accdir, hf_prefix)

if rank == 0:

    # copy line not starting with '#' into a local FD_STATLIST copy
    # file pointers for each processes' FD_STATLIST portion
    # number of stations for each process
    nss = [0] * jobs
    stats_per_job = int(math.ceil(stat_estimate/float(jobs))) #this will be 1 if rand_reset is True as job=stat_estimate above
    #ceiling may cause stats_per_job* jobs >= stat_estimate. (eg. 13/4 yielding 4). This may cause IndexError (handled below), but ensures all stations are correctly processed.

    for i, f in enumerate(statfiles):
        with open(f,'w') as fp:
            for j in range(stats_per_job):
                try:
                    fp.write(all_stations[i*stats_per_job+j])
                except IndexError:
                    break #i*stats_per_job+j may exceed "stat_estimate". Break for-j here and let for-i continue
                else:
                    nss[i]+=1
            fp.write('\n')

    # some info echoed in original script, might be useful for some
    if not site_specific:
        print("%d %s" % (sum(nss), hf_v_model))

#def run_hf(rank,local_statfile, n_stat):
def run_hf((local_statfile,n_stat)):
    global hf_v_model
    print "Rank %d Processing %s containing %d stations" %(rank, local_statfile,n_stat)

    # just in case more processes than stations
    if n_stat == 0:
        return

    #if site_specific=True, hf_v_model needs to be worked out 
    if site_specific:
#        if rank == 0 :
#            print "Site specific 1D profile"
        if n_stat > 1:
            print "Error: For site specific, n_stat = 1 must be met. Check %s" %local_statfile
            comm.Abort()
        with open(local_statfile,'r') as fr:
            lines=fr.readlines() #this must be non-empty. n_stat > 0 as handled above

        stat=lines[0]
        statname = stat.split(' ')[-1].strip('\n')
        hf_v_model = os.path.join(hf_v_model_path,'%s.1d'%statname)
        
#    else:
#        if rank == 0:
#            print "Universal 1D profile"

    cmd = hf_sim_bin
    args='\n%s\n%s\n%s\n%s\n%s\n4   0  0.02  19.9\n%s\n%s\n%s %s %s %s %s\n%s %s %s %s %s\n%s %s\n%s\n%s\n%s\n-99 0.0 0.0 0.0 0.0 1\n-1\n%s 0.0 %s\n%s\n' % (hf_sdrop, local_statfile, out_prefix, hf_rayset, hf_site_amp, hf_seed, n_stat, hf_t_len, hf_dt, hf_fmax, hf_kappa, hf_qfexp, hf_rvfac, hf_shal_rvfac, hf_deep_rvfac, hf_czero, hf_calpha, hf_mom, hf_rupv, hf_slip, hf_v_model, hf_vs_moho, hf_fa_sig_1, hf_rv_sig_1, hf_path_dur)

 
   # prevent terminal echo slow down, not useful with many processes anyway
#    sink = open('/dev/null', 'w')
    #sink = sys.stdout
    sink = open(os.path.join(hf_sim_dir,'%s.out'%local_statfile), 'w')
    sink.write("%s %s\n"%(cmd,args))
    sink.write("1D profile used: %s\n" %hf_v_model)


 # execute calculation, stdin from pipe
    hf_pipe = Popen([cmd], stdin = PIPE, stdout = sink)
    # pipe input data into binary stdin
    # look at hf_sim_bin source code (same dir) for more info on parameters
    hf_pipe.communicate(args)
    sink.close()

#p = Pool(size)
#p.map(run_hf, zip(statfiles, nss))
if rank == 0:
    stat_data = zip(statfiles, nss)
else:
    stat_data = [None,0]


#statfile, nss = comm.scatter(stat_data, root=0) #all processes will be waiting for rank 0 to scatter data. No barrier needed

comm.Barrier()

if rand_reset:
    executor = parallel_executor.ParallelExecutor()
    executor.process_function_with_result(run_hf,stat_data)
else:
    statfile, nss = comm.scatter(stat_data, root=0)
    print "rank %d" % rank
    run_hf((statfile,nss))

    

if rank == 0:
#    for temp_statfile in statfiles:
#        os.remove(temp_statfile)

    set_permission(hf_sim_dir)


