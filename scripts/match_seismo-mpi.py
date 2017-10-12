#!/usr/bin/env python2
"""
Takes LF/HF seismograms and creates BB motions using a matched filter.

USAGE:
The specific variables that generally need to be changed on each run are:
 - `stat_file` - stations considered, also `stat_vs_ref` and `stat_vs_est`
 - `hf_sim_dir` and `hf_prefix` - directory and prefix of the HF seismograms
 - `vel_dir` - the dir of the LF vel seismograms
 - `site_amp_model` - choose the site amplification factor model to use

Converted to Python.
@date 06 May 2016
@author Viktor Polak
@contact viktor.polak@canterbury.ac.nz
"""
import sys
import os
sys.path.append(os.path.abspath(os.path.curdir))
from shutil import copyfile
from subprocess import call, Popen, PIPE
from shared import *
from params import *
from params_base_bb import *
import glob
from mpi4py import MPI

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()


if len(sys.argv)!=2:
    if rank == 0:
        print "Usage: %s bb_sim_dir" %sys.argv[1]
        print "Note that bb_sim_dir is where the output will be stored"
    comm.Abort()

bb_sim_dir = sys.argv[1]

sys.path.append(bb_sim_dir)
from params_bb_uncertain import *
# working files
tmp_filelist = os.path.join(bb_sim_dir,'temp.filter_list%d'%rank)
lf_copy = os.path.join(bb_sim_dir,'temp.lf%d'%rank)
hf_copy = os.path.join(bb_sim_dir,'temp.hf%d'%rank)
match_log_full = os.path.join(bb_sim_dir,'%s%d'%(match_log,rank))

if rank == 0:
    # verify input incl. params.py
    verify_binaries([int_bin, siteamp_bin, tfilter_bin, match_bin, getpeak_bin])
    verify_files([stat_file, stat_vs_ref, stat_vs_est])
    verify_logfiles([match_log_full, tmp_filelist, lf_copy, hf_copy])
    verify_strings([hf_prefix, match_hf_fhi, match_hf_flo, match_hf_ord, match_hf_tstart, \
            match_lf_fhi, match_lf_flo, match_lf_ord, match_lf_tstart, site_amp_model, \
            site_amp_model, site_vref_max, site_fmin, site_fmidbot, site_flowcap, \
            site_flowcap, site_fmid, site_fhigh, site_fhightop, site_fmax,\
            GEN_ROCK_VS])
    verify_lists([match_hf_comps, match_lf_comps])
    verify_user_dirs([bb_sim_dir,hf_accdir, vel_dir])

try:
    verify_strings([bb_resume])
except: #not defined in params_bb_uncertain
    bb_resume = False
finally:
    if rank == 0:
        print "bb_resume = %s" %bb_resume

if rank == 0:
    verify_user_dirs([bb_accdir, bb_veldir], reset=not bb_resume) #output directories are best to start from scratch

logger = open(match_log_full, 'w')

#stations, station_lats, station_lons = get_stations(stat_file, True)
stations, station_lats, station_lons = get_stations(FD_STATLIST, True)

# source file (parameter) must contain Vs30 values
vrefs = get_vs(stat_vs_ref)

try:
    verify_files([hf_stat_vs_ref])
except: #old behaviour - no site specific vs30ref
    if rank == 0: 
        print "No site specific Vs30ref for HF"
    hf_vrefs = None
else:
    if rank == 0:
        print "Site specific Vs30ref for HF"
    hf_vrefs = get_vs(hf_stat_vs_ref) #we have site specific vs30ref

# Vs30 for sites
vsites = get_vs(stat_vs_est)



# for power spectrum based filtering
if match_powersb:
    # doesn't make sense if the orders, freqs don't match
    try:
        assert(match_hf_ord == match_lf_ord)
        assert(match_hf_fhi == match_lf_flo)
    except AssertionError:
        if rank == 0:
            print('HF and LF orders must match, pass at same frequency.')
        raise
    if match_hf_ord == '4':
        match_hf_fhi = '%.16f' % (float(match_hf_fhi) * 0.8956803352330285)
        match_lf_flo = '%.16f' % (float(match_lf_flo) * 1.1164697500474103)
    else:
        from math import exp
        # full formula:
        # orig fhi, flo *= exp((+-1.0 / (2.0 * order)) * log(sqrt(2.0) - 1.0))
        match_hf_fhi = '%.16f' % (float(match_hf_fhi) * \
                exp(1.0 / (2.0 * int(match_hf_ord)) * -0.8813735870195428))
        match_lf_flo = '%.16f' % (float(match_lf_flo) * \
                exp(-1.0 / (2.0 * int(match_lf_ord)) * -0.8813735870195428))

# file list is passed to binaries but will only ever contain one item
def set_filelist(filename):
    with open(tmp_filelist, 'w') as fp:
        fp.write(filename)

if bb_resume:
    size = 1 #number of processors.
    ext='.000'
    old_vel_files = glob.glob(os.path.join(bb_veldir,'*%s'%ext))
    old_vel_samples = old_vel_files[:size+1]
    old_acc_files = glob.glob(os.path.join(bb_accdir,'*%s'%ext))
    old_acc_samples = old_acc_files[:size+1]
    acc_file_sizes = [os.stat(v).st_size for v in old_acc_samples]
    vel_file_sizes = [os.stat(v).st_size for v in old_vel_samples]
    if len(acc_file_sizes) == 0 or len(vel_file_sizes) == 0:
        bb_resume = False
    else:
        normal_acc_size = max(acc_file_sizes)
        normal_vel_size = max(vel_file_sizes)
        if rank  == 0:
            print "Normal Acc file size: %d bytes Normal Vel file size: %d bytes" %(normal_acc_size, normal_vel_size)

for s_index, stat in enumerate(stations):
    if s_index % size != rank: #eg. size = 4, rank 1 only processes s_index 1, 5, 9...
        continue
        
    # assuming vrefs are whole numbers, seems to hold
    vref = str(min(site_vref_max, int(vrefs[stat])))
    vsite = vsites[stat]
    if hf_vrefs is None:
        hf_vref = GEN_ROCK_VS
    else:
#        hf_vref = str(min(site_vref_max, int(hf_vrefs[stat]))) # keep hf_vref under 1100 (site_vref_max)
        hf_vref = hf_vrefs[stat]

    if bb_resume:
        exists=True
        for comp in match_lf_comps:
            vel_file = os.path.join(bb_veldir,'%s.%s'%(stat,comp))
            acc_file = os.path.join(bb_accdir,'%s.%s'%(stat,comp))

            exists = exists and os.path.exists(vel_file) and os.path.exists(acc_file) and os.stat(vel_file).st_size == normal_vel_size and os.stat(acc_file).st_size == normal_acc_size
        if exists:
            print "Station %s has been already processed: Skipping" %stat
            continue #skip
        
    # set everything to same Vs30, i.e., no site amp - need to change this. remove?
    #vref = GEN_ROCK_VS
    #vsite = GEN_ROCK_VS
    print('===========================')
    print('rank=%d %s %s %s %s %s' % \
            (rank, stations[s_index], station_lons[s_index], station_lats[s_index], vref, vsite))
    print('===========================')
    for c_index, comp in enumerate(match_lf_comps):
        # working copy of HF file
        copyfile(os.path.join(hf_accdir, \
                '%s_%s.%s' % (hf_prefix, stat, match_hf_comps[c_index])), hf_copy)
        set_filelist(hf_copy)
        # get PGA (needed to determine the level of NL site amp)
        hfp = open(hf_copy, 'rb')
        peak_pipe = Popen([getpeak_bin], stdin = PIPE, stdout = PIPE)
        peak_output, peak_err = peak_pipe.communicate(hfp.read())
        hfp.close()
        pga = float(peak_output.split()[1])
        pga = '%.6f' % (pga / 981.0)
        # debug
        logger.write('%s %s %s %s %s\n' % (stat, comp, pga, vref, vsite))
        print('   -> %s %s' % (comp, pga))
        # apply site amplification to HF record
        exe([siteamp_bin, 'pga=%s' % pga, 'vref=%s' % hf_vref, 'vsite=%s' % vsite, \
                'model=%s' % site_amp_model, 'vpga=%s' % hf_vref, 'flowcap=%s' % site_flowcap, \
                'infile=%s' % hf_copy, 'outfile=%s' % hf_copy, \
                'fmidbot=%s' % site_fmidbot, 'fmin=%s' % site_fmin, \
                'fhigh=%s' % site_fhigh, 'fhightop=%s' % site_fhightop, \
                'fmax=%s' % site_fmax])
        # filter HF record
        exe([tfilter_bin, 'filelist=%s' % tmp_filelist, 'order=%s' % match_hf_ord, \
                'fhi=%s' % match_hf_fhi, 'flo=%s' % match_hf_flo, \
                'inbin=0', 'outbin=0', 'phase=0', 'outpath=%s' %bb_sim_dir])

        # working copy of LF file
        copyfile(os.path.join(vel_dir, '%s.%s' % (stat, comp)), lf_copy)
        set_filelist(lf_copy)
        # differentiate LF Vel to get LF Acc
        exe([int_bin, 'diff=1', 'inbin=0', 'outbin=0', \
                'filein=%s' % lf_copy, 'fileout=%s' % lf_copy])
        # apply site amplification to LF record
        exe([siteamp_bin, 'pga=%s' % pga, 'vref=%s' % vref, 'vsite=%s' % vsite, \
                'model=%s' % site_amp_model, 'vpga=%s' % vref, 'flowcap=%s' % site_flowcap, \
                'infile=%s' % lf_copy, 'outfile=%s' % lf_copy, \
                'fmidbot=%s' % site_fmidbot, 'fmin=%s' % site_fmin, \
                'fhigh=%s' % site_fhigh, 'fhightop=%s' % site_fhightop, \
                'fmax=%s' % site_fmax])

        # filter LF record
        exe([tfilter_bin, 'filelist=%s' % tmp_filelist, 'order=%s' % match_lf_ord, \
                'fhi=%s' % match_lf_fhi, 'flo=%s' % match_lf_flo, \
                'inbin=0', 'outbin=0', 'phase=0', 'outpath=%s'%bb_sim_dir])

        # combine LF and HF using matched fileters
        exe([match_bin, 'f1=1.00', 't1=%s' % match_lf_tstart, 'inbin1=0', \
                'infile1=%s' % lf_copy, 'f2=1.00', 't2=%s' % match_hf_tstart, \
                'inbin2=0', 'infile2=%s' % hf_copy, 'outbin=0', \
                'outfile=%s%s%s.%s' % (bb_accdir, os.path.sep, stat, comp)])
        # integrate the BB ACC to get BB VEL
        file_in = os.path.join(bb_accdir, '%s.%s' % (stat, comp))
        file_out = os.path.join(bb_veldir, '%s.%s' % (stat, comp))
        print "file_in=%s \nfile_out=%s" %(file_in,file_out)

        exe([int_bin, 'integ=1', 'filein=%s' % file_in, \
                'inbin=0', 'outbin=0', 'fileout=%s' % file_out])

logger.close()
# remove temp files
os.remove(tmp_filelist)
os.remove(hf_copy)
os.remove(lf_copy)
set_permission(bb_sim_dir)

