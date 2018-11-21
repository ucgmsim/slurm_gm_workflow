#!/usr/bin/env python2

from os import path

from qcore.geo import *
from shared_workflow.shared import *
import sys
import os

sys.path.append(os.path.abspath(os.path.curdir))
from params import *


def main(stat_file = 'default.ll', \
        MODEL_LAT = MODEL_LAT, MODEL_LON = MODEL_LON, MODEL_ROT = MODEL_ROT, \
        hh = hh, nx = nx, ny = ny, sim_dir = sim_dir, sufx = sufx, \
        debug = False):
    verify_strings([MODEL_LAT, MODEL_LON, MODEL_ROT, hh, nx, ny,sim_dir,sufx])

    outpath = sim_dir
    filename = 'fd%s'%sufx

    print stat_file

    # arbitrary longlat station input
    ll_in = stat_file
    # where to save gridpoint and longlat station files
    gp_out = path.join(outpath, '%s.statcords' % filename)
    ll_out = path.join(outpath, '%s.ll' % filename)


    print "From: %s" %stat_file
    print "To:"
    print "  %s" %gp_out
    print "  %s" %ll_out

    # velocity model parameters
    nx = int(nx)
    ny = int(ny)
    mlat = float(MODEL_LAT)
    mlon = float(MODEL_LON)
    mrot = float(MODEL_ROT)
    hh = float(hh)

    # retrieve in station names, latitudes and longitudes
    sname, slat, slon = get_stations(ll_in, locations = True)
    slon = map(float, slon)
    slat = map(float, slat)

    # convert ll to grid points
    xy = ll2gp_multi(map(list, zip(slon, slat)), \
            mlon, mlat, mrot, nx, ny, hh, keep_outside = True)

    # store gridpoints and names if unique position
    sxy = []
    suname = []
    for i in xrange(len(xy)):
        if xy[i] is None:
            if debug:
                print('Station outside domain: %s' % (sname[i]))
        elif xy[i] not in sxy:
            sxy.append(xy[i])
            suname.append(sname[i])
        else:
            if debug:
                print('Duplicate Station Ignored: %s' % (sname[i]))

    # create grid point file
    with open(gp_out, 'w') as gpf:
        # file starts with number of entries
        gpf.write('%d\n' % (len(sxy)))
        # x, y, z, name
        for i, xy in enumerate(sxy):
            gpf.write('%5d %5d %5d %s\n' % (xy[0], xy[1], 1, suname[i]))

    # convert unique grid points back to ll
    # warning: modifies sxy
    ll = gp2ll_multi(sxy, mlat, mlon, mrot, nx, ny, hh)

    # create ll file
    with open(ll_out, 'w') as llf:
        # lon, lat, name
        for i, pos in enumerate(ll):
            llf.write('%11.5f %11.5f %s\n' % (pos[0], pos[1], suname[i]))

    return gp_out, ll_out

if __name__ == '__main__':
    main(stat_file = stat_file)

