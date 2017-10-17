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
sys.path.append(os.path.abspath(os.path.curdir))
from subprocess import call, Popen, PIPE
from shared import *
from params import *
from params_base_bb import *

import glob

sys.path.append('/nesi/projects/nesi00213/qcore')

import remaining_stations

rand_reset = False
site_specific = False
option = 0
if len(sys.argv)<3:
    print "Usage: %s hf_sim_dir [0-2] " %sys.argv[0]
    print "- hf_sim_dir is where the output will be stored"
    print "- Option:"
    print "        0 (default): may produce different result for different num procs."
    print "        1: rand_reset, enforce random number sequence reset for each station"
    print "        2: site_specific, seperate 1d profile for each station"
    sys.exit()
if len(sys.argv)>=3:
    option = int(sys.argv[2])

if option >= 1:
    rand_reset = True
if option >= 2:
    site_specific = True


print "Rand Reset:%s" %rand_reset
print "Site Specific:%s" %site_specific
hf_sim_dir = sys.argv[1]


sys.path.append(hf_sim_dir)
from params_bb_uncertain import *
try:
    from params_bb_uncertain import hf_kappa, hf_sdrop
except ImportError:
    print "Note: hf_kappa and hf_sdrop appear to be in params.py - old style"
finally:
    print "hf_kappa: %s"
    print "hf_sdrop: %s"

try:
    from params_base_bb import hf_v_model
except ImportError:
    if not site_specific:
        print "Error: hf_v_model should be in params_bb for non site_specific computation"
        sys.exit()



# verify input incl. params.py
verify_binaries([hf_sim_bin])
#verify_files([stat_file, hf_slip, hf_v_model])

if site_specific:
    verify_files([FD_STATLIST, hf_slip])
    verify_user_dirs([hf_v_model_path])
    hf_v_model = None
else:
    verify_files([FD_STATLIST, hf_slip, hf_v_model])


verify_strings([hf_prefix, hf_t_len, hf_dt, hf_vs_moho, hf_fa_sig_1, hf_rv_sig_1, \
        hf_sdrop, hf_kappa, hf_qfexp, hf_rayset, hf_rvfac, hf_shal_rvfac, hf_deep_rvfac, \
        hf_czero, hf_site_amp, hf_mom, hf_rupv, hf_seed, hf_fmax, hf_calpha, hf_path_dur])
verify_user_dirs([hf_sim_dir])

try:
    verify_strings([hf_resume])
except:
    hf_resume = False

verify_user_dirs([hf_accdir],reset=not hf_resume) #to resume from the checkpoint, reset=False. The combo of hf_resume=True and rand_reset=False is not recommended for reproducibility


out_prefix = os.path.join(hf_accdir, hf_prefix)


all_stations = remaining_stations.get_lines(FD_STATLIST,hf_accdir,1)

n_statfiles = 1
if rand_reset:
    n_statfiles = len(all_stations)
n_stats=[0]*n_statfiles

local_statfiles=[]
for i in range(n_statfiles):
    local_statfile = os.path.join(hf_sim_dir, 'local.tmp.sf_%.5d'%i)
    if rand_reset: #1 stat in 1 file
        with open(local_statfile,'w') as lp:
            lp.write(all_stations[i])
            lp.write('\n')
            n_stats[i]+=1
    else: #all in one file
        with open(local_statfile,'w') as lp:
            for j,stat in enumerate(all_stations):
                lp.write(all_stations[j])
                n_stats[i]+=1
            lp.write('\n')
    local_statfiles.append(local_statfile)


for i,local_statfile in enumerate(local_statfiles):
    n_stat = n_stats[i]
    if n_stat == 0:
        continue 
    if site_specific:
#        print "Site specific 1D profile"
        if n_stat > 1:
            print "Error: For site specific, n_stat = 1 must be met. Check %s" %local_statfile
            sys.exit()

        with open(local_statfile,'r') as fr:
            lines = fr.readlines() 
        stat = lines[0]
        statname = stat.split(' ')[-1].strip('\n')
        hf_v_model = os.path.join(hf_v_model_path,'%s.1d'%statname)
#    else:
#        print "Universal 1D profile"

   # prevent terminal echo slow down, not useful with many processes anyway
#    sink = open('/dev/null', 'w')
    #sink = sys.stdout
    cmd = hf_sim_bin
    args = '\n%(hf_sdrop)s\n%(local_statfile)s\n%(out_prefix)s\n%(hf_rayset)s\n%(hf_site_amp)s\n4   0  0.02  19.9\n%(hf_seed)s\n%(n_stat)s\n%(hf_t_len)s %(hf_dt)s %(hf_fmax)s %(hf_kappa)s %(hf_qfexp)s\n%(hf_rvfac)s %(hf_shal_rvfac)s %(hf_deep_rvfac)s %(hf_czero)s %(hf_calpha)s\n%(hf_mom)s %(hf_rupv)s\n%(hf_slip)s\n%(hf_v_model)s\n%(hf_vs_moho)s\n-99 0.0 0.0 0.0 0.0 1\n-1\n%(hf_fa_sig_1)s 0.0 %(hf_rv_sig_1)s\n%(hf_path_dur)s\n' % locals()

    sink = open(os.path.join(hf_sim_dir,'%s.out'%local_statfile), 'w')
    sink.write("%s %s\n"%(cmd,args))
    sink.write("1D profile used: %s\n" %hf_v_model)

    # execute calculation, stdin from pipe
    hf_pipe = Popen([cmd], stdin = PIPE,stdout=sink)
    # pipe input data into binary stdin
    # look at hf_sim_bin source code (same dir) for more info on parameters
    hf_pipe.communicate(args)
    sink.close()

set_permission(hf_sim_dir)
