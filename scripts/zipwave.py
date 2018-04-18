#!/usr/bin/env python2
"""
A pattern template to attempt an efficient mpi4py program.
"""

from glob import glob
import math
import os
import sys
from time import time, sleep
import zipfile

from mpi4py import MPI

MASTER = 0
BATCH_SIZE = 500

def get_basenames(acc_vel_folder):
    """
    Find Acc and Vel station basenames.
    """
    def noext(path):
        return os.path.splitext(path)[0]
    return map(noext, glob('%s/*.ver' % (acc_vel_folder)))

def zip_stations(station_paths):
    for basename in station_paths:
        try:
            # test if zip is just an empty file
            if os.stat('%s.zip' % (basename)).st_size:
                continue
        except OSError:
            # no zip
            pass
        try:
            with zipfile.ZipFile('%s.zip' % (basename), mode = 'w', \
                                        compression = zipfile.ZIP_DEFLATED, \
                                        allowZip64 = True) as station_zip:
                for axis in ['.000', '.090', '.ver']:
                    station_zip.write('%s%s' % (basename, axis), \
                            arcname = '%s%s' % (os.path.basename(basename), axis))
        except IOError:
            print('failed to create %s.zip' % (basename))

def reporting(reports, timing):
    """
    Display summary of work done.
    """
    print('%s SUMMARY %s' % ('=' * 10, '=' * 10))
    print('num slaves: %d' % (len(reports)))
    a_time = 0.0
    b_time = 0.0
    s_time = 0.0
    for slave_report in reports:
        for job in slave_report:
            if job[0] == 'station_file_search':
                a_time += job[1]
            if job[0] == 'station_zips':
                b_time += job[1]
            elif job[0] == 'sleep':
                s_time += job[1]
    print('station_file_search total: %.2fs' % (a_time))
    print('station_zips total: %.2fs' % (b_time))
    print('wasted time total: %.2fs' % (s_time))
    print('setup time: %.2fs, working time %.2fs' \
            % (timing[1] - timing[0], timing[2] - timing[1]))
    print('speedup factor: %.2fx faster than sequential' \
            % (((a_time + b_time) + (timing[1] - timing[0]) * len(reports)) \
            / (timing[2] - timing[0])))

###
### MASTER
###
if len(sys.argv) > 1:
    t_master = MPI.Wtime()

    # setup MPI parameters and initial dependency-free jobs
    sys_procs = os.sysconf('SC_NPROCESSORS_ONLN')
    size = int(sys.argv[1])
    cybershake_root = sys.argv[2]
    assert(os.path.isdir(cybershake_root))
    acc_vel = glob('%s/Runs/*/GM/Sim/Data/BB/*/*/Acc' % (cybershake_root))
    acc_vel.extend(glob('%s/Runs/*/GM/Sim/Data/BB/*/*/Vel' % (cybershake_root)))
    jobs = [(get_basenames, folder) for folder in acc_vel]
    deps = len(jobs)

    # spawn slaves
    comm = MPI.COMM_WORLD.Spawn(
        sys.executable, args = [sys.argv[0]], maxprocs = size)

    # job tracking
    in_progress = [None] * size

    status = MPI.Status()
    t_mpi = MPI.Wtime()
    while size:
        # wait for slave, get return value, id, previously completed function
        r = comm.recv(source = MPI.ANY_SOURCE, status = status)
        slave_id = status.Get_source()
        finished = in_progress[slave_id]

        # dependency tracking
        if finished == None:
            pass
        elif finished[0] == get_basenames:
            batches = int(math.ceil(len(r) / float(BATCH_SIZE)))
            for batch in xrange(batches):
                jobs.append((zip_stations, r[batch::batches]))
            deps += batches
            deps -= 1
        elif finished[0] == zip_stations:
            deps -= 1

        try:
            # look for work to give slave
            msg = jobs[0]
            del(jobs[0])
        except IndexError:
            if deps == 0:
                # slave is no longer needed, kill it
                msg = StopIteration
                size -= 1
            else:
                # we don't have work for the slave (yet)
                msg = None

        comm.send(obj = msg, dest = slave_id)
        in_progress[slave_id] = msg

    t_end = MPI.Wtime()
    # gather reports
    reports = comm.gather(None, root = MPI.ROOT)
    # shutdown
    comm.Disconnect()
    # print reports
    reporting(reports, (t_master, t_mpi, t_end))

###
### SLAVE
###
else:
    # connect and test comm
    comm = MPI.Comm.Get_parent()
    try:
        rank = comm.Get_rank()
    except MPI.Exception as e:
        if e.Get_error_code() == 5:
            # MPI_ERR_COMM
            sys.exit('First parameter is number of processes.')
        else:
            sys.exit(e.error_string)

    logbook = []
    r = None
    for job in iter(lambda: comm.sendrecv(r, dest = MASTER), StopIteration):
        t0 = time()
        r = None

        if job == None:
            # no work to do yet, wait for dependency to create more work
            sleep(1)
            logbook.append(('sleep', time() - t0))

        elif job[0] == get_basenames:
            r = job[0](job[1])
            logbook.append(('station_file_search', time() - t0))

        elif job[0] == zip_stations:
            r = job[0](job[1])
            logbook.append(('station_zips', time() - t0))

        else:
            print('Unknown Job: %s' % (job))

    # reports to master
    comm.gather(sendobj = logbook, root = MASTER)
    # shutdown
    comm.Disconnect()
