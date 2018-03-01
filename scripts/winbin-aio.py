#!/usr/bin/env python2
"""
Generates ASCII format version of seis file for each station/component.

@author Viktor Polak, Sung Bae
@date 5 April 2016

Replaces winbin-aio.csh. Re implemented in python including main binary.

USAGE: execute from current directory being the simulation directory.

ISSUES: think of better filename

10 June 2016:
Include logic of fdbin2wcc and wcc_rotate binaries as python.
Removes dependencies of (very) slow and inefficient programs.
scale variable assumed to always be 1.0 (no reason to use other values?)
MODEL_ROT read directly from seis file, saves depending on variables
TSTRT assumed to be 0 (start at t=0), need to add functionality if needed.

16 January 2017:
Use memory mapping to prevent out-of-memory for large seis files
caused by many stations (virtual stations), higher number of timeslices
Use updated workflow params, not e3d.par.
"""

from glob import glob
from math import sin, cos, radians
import os
import sys
sys.path.append(os.path.abspath(os.path.curdir))

import numpy as np

from qcore.shared import *
from shared_bin import *

from params_base import *
from shared_workflow import remaining_stations

if len(sys.argv) > 1:
    if os.path.exists(sys.argv[1]) and os.path.isdir(sys.argv[1]):
        path_to_add = os.path.abspath(sys.argv[1])
        sys.path.append(path_to_add)
        print path_to_add
        from params_uncertain import *
    if sys.argv[1] == 'test_mode':
       print('Running under test mode.')
       from postprocess_test.test_params import *


# folder containing OutBin, Vel, TSlice etc...
# LF can have multiple SRF
## the following info should be available in params_uncertain
#lf_base = os.path.abspath(sys.argv[1])
#bin_output = os.path.join(lf_base, 'OutBin')
#vel_dir = os.path.join(lf_base, 'Vel')
# stations to extract Velocities for

try:
    verify_strings([lf_vel_resume]) #if lf_vel_resume=True in params_uncertain, we resume
except NameError:
    lf_vel_resume = False #by default it is False


verify_files([FD_STATLIST])
verify_user_dirs([vel_dir], reset = not lf_vel_resume)
verify_dirs([bin_output])

# stations to retrieve from seis files
#stats = get_stations(stat_file) #stat_file is unadjusted superset of FD_STATLIST which may contain stations out of domain
stats = get_stations(FD_STATLIST)
print "Total of %d stations in station file" %len(stats)
if lf_vel_resume:
    stats = remaining_stations.get_codes(stats,vel_dir)
    print "Resuming enabled. Computing %d remaining stations" %(len(stats))
else:
    print "Resuming disabled. Computing all %d stations" %(len(stats))

# just get all seis files (should all be relevant)
filepattern = os.path.join(bin_output, '*_seis*.e3d')
seis_file_list = sorted(glob(filepattern))

# python data-type format string
INT_S = 'i'
FLT_S = 'f'
# if non-native input, prepend '>' big source, or '<' little source
if get_seis_swap(seis_file_list[0]):
    swapping_char = get_byteswap_char()
    INT_S = swapping_char + INT_S
    FLT_S = swapping_char + FLT_S

# read common data only from first file
fp = open(seis_file_list[0], 'rb')
read_flt = lambda : unpack(FLT_S, fp.read(SIZE_FLT))[0]
fp.seek(SIZE_INT * 5)
nt = unpack(INT_S, fp.read(SIZE_INT))[0]
dt = read_flt()
hh = read_flt()
rot = read_flt()
fp.close()
# rotation matrix for converting to 090, 000
# ver is inverted (* -1)
theta = radians(rot)
rot_matrix = np.array([[ cos(theta), -sin(theta),  0], \
                       [-sin(theta), -cos(theta),  0], \
                       [          0,           0, -1]])

# each seis_file has a subset of stations
for si, seis_file in enumerate(seis_file_list):
    print('processing input file %d of %d...' \
            % (si + 1, len(seis_file_list)))
    fp = open(seis_file, 'rb')
    # speed optimise
    seek = fp.seek
    read = fp.read

    # file starts with number of stations
    num_stat = unpack(INT_S, read(SIZE_INT))[0]
    # component data is stored at end, may be too big to fit in RAM
    # looping order: timestep, stations, components
    comp_data = np.memmap(seis_file, dtype = FLT_S, mode = 'r', \
            offset = SIZE_INT + num_stat * SIZE_SEISHEAD, \
            shape = (nt, num_stat, N_COMPS))

    # skip through station names, check if wanted station
    for stat_i in xrange(num_stat):
        seek(SIZE_INT + (stat_i + 1) * SIZE_SEISHEAD - STAT_CHAR)
        stat = str(read(STAT_CHAR)).rstrip('\0')
        if stat in stats:
            # easy way to check for unfound stations
            stats.remove(stat)

            # all timeslices, for this stat, comp 0, 1 and 2
            # rotate to produce x, y, -z from arbitrary rotation
            comps = np.dot(comp_data[:, stat_i, :3], rot_matrix)

            # store as vel files
            for i, comp in enumerate(MY_COMPS.values()):
                write_seis_ascii(vel_dir, comps[:, i], stat, comp, nt, dt)
    fp.close()

if len(stats) > 0:
    print('WARNING: the following stations were not found: %s' \
            % ', '.join(stats))
