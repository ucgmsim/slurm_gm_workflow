"""
Module which contains shared functions/values.
Only relating to binary files and C code IO manipulations.

@date 10 June 2016
@author Viktor Polak
@contact viktor.polak@canterbury.ac.nz
"""

from os import stat
from struct import unpack
from sys import byteorder

import numpy as np

###
### binary properties
###
# 8 byte c-string has up to 7 characters, followed by at least one '\0'
STAT_CHAR = 8
# a station with a name of this length is virtual
VSTAT_LEN = 7
# number of bytes per value, int/float = 4 Byte on 64 bit machines
SIZE_INT = 4
SIZE_FLT = 4
# number of bytes of station info in seis file
SIZE_SEISHEAD = SIZE_INT * 5 + SIZE_FLT * 5 + STAT_CHAR
# seis format properties
N_COMPS = 9
# values of interest in components, index in N_COMPS, description
# changing these requires changing logic
MY_COMPS = {0:'090', 1:'000', 2:'ver'}
N_MY_COMPS = len(MY_COMPS)
# ASCII properties - values per line
VPL = 6

NATIVE_ENDIAN = byteorder

# return order to read bytes in, either '>' or '<'
# assumes you want to read in the non-native order
def get_byteswap_char():
    if byteorder == 'little':
        # process as big endian
        return '>'
    else:
        # process as little endian
        return '<'

# automatic endianness detection for seis files
# first assume that endianness is native, check if filesize matches
def get_seis_swap(file_path):
    fp = open(file_path, 'rb')
    # number of stations in seis file
    ns = unpack('i', fp.read(SIZE_INT))[0]
    # first station NT value (should all be same)
    fp.seek(SIZE_INT + 4 * SIZE_INT)
    nt = unpack('i', fp.read(SIZE_INT))[0]

    # assuming correct values read, this is the filesize
    fs = SIZE_INT + ns * (SIZE_SEISHEAD + SIZE_FLT * N_COMPS * nt)

    if fs == stat(fp.name).st_size:
        return False
    return True

def get_seis_common(file_path, INT_S, FLT_S):
    """
    Reads seis station information which should be
    common to every station in a simulation.

    INT_S: format string of an integer
    FLT_S: format string of a float
    """
    fp = open(file_path, 'rb')
    # skip to first station contents
    fp.seek(SIZE_INT * 5)
    nt = unpack(INT_S, fp.read(SIZE_INT))[0]
    # expected to not have numbers past 6 decimal places
    # prevents float representation errors
    dt = round(unpack(FLT_S, fp.read(SIZE_INT))[0], 6)
    hh = round(unpack(FLT_S, fp.read(SIZE_INT))[0], 6)
    rot = round(unpack(FLT_S, fp.read(SIZE_INT))[0], 6)

    return nt, dt, hh, rot

# write data to ASCII seis file
def write_seis_ascii(out_dir, data, stat, comp, nt, dt, \
        t_hr = 0, t_min = 0, t_sec = -1.0, \
        edist = 0.0, az = 0.0, baz = 0.0, title = ''):
    # no trailing 0s in ASCII header (as in wcc_rotate behaviour)
    try:
        comp_header = str(int(comp))
    except ValueError:
        comp_header = comp

    op = open('%s/%s.%s' % (out_dir, stat, comp), 'w')
    # same format strings as fdbin2wcc
    op.write('%-10s %3s %s\n' % (stat, comp_header, title))
    op.write('%d %12.5e %d %d %12.5e %12.5e %12.5e %12.5e\n' % \
            (nt, dt, t_hr, t_min, t_sec, edist, az, baz))
    # values below header lines
    for full_line_i in xrange(nt // VPL):
        op.write('%13.5e%13.5e%13.5e%13.5e%13.5e%13.5e\n' % \
                tuple(data[full_line_i * VPL:full_line_i * VPL + VPL]))
    # partial last line
    if nt % VPL != 0:
        for t in xrange(nt - nt % VPL, nt):
            op.write('%13.5e' % (data[t]))
        op.write('\n')
    op.close()

