#!/usr/bin/env python
"""
Simulates high frequency seismograms for stations.
"""
from argparse import ArgumentParser
import os
import random
from subprocess import Popen, PIPE
import sys
from tempfile import mkstemp

from mpi4py import MPI
import numpy as np
import logging

from shared_workflow import shared_defaults
from qcore import binary_version, MPIFileHandler, constants

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()
master = 0
is_master = not rank
HEAD_SIZE = 0x0200
HEAD_STAT = 0x18
FLOAT_SIZE = 0x4
N_COMP = 3
MAX_STATLIST = 10

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
velocity_name = "-1"


logger = logging.getLogger("rank_%i" % comm.rank)
logger.setLevel(logging.DEBUG)


def random_seed():
    return random.randrange(1000000, 9999999)


args = None
if is_master:
    parser = ArgumentParser()
    arg = parser.add_argument
    # HF IN, line 12
    arg("--slip", required=True, dest="stoch_file", help="rupture model")
    # HF IN, line 2
    arg("station_file", help="station file (lon, lat, name)")
    # HF IN, line 3
    arg("out_file", help="file path for HF output")
    # ARG 0
    arg(
        "--sim_bin",
        help="high frequency binary (modified for binary out)",
        default=binary_version.get_hf_binmod("5.4.5"),
    )
    arg("--version",
        help="binary version, similar to --sim_bin but not full path.",
        default=None)
    arg("--t-sec", help="high frequency output start time", type=float, default=0.0)
    # HF IN, line 1
    arg("--sdrop", help="stress drop average (bars)", type=float, default=50.0)
    # HF IN, line 4
    arg(
        "--rayset",
        help="ray types 1:direct 2:moho",
        nargs="+",
        type=int,
        default=[1, 2],
    )
    # HF IN, line 5
    arg(
        "--no-siteamp",
        help="disable BJ97 site amplification factors",
        action="store_true",
    )
    # HF IN, line 7
    arg(
        "--seed",
        help="random seed (0:randomised reproducible, -1:fully randomised)",
        type=int,
        default=0,
    )
    # HF IN, line 9
    arg("--duration", help="output length (seconds)", type=float, default=100.0)
    arg("--dt", help="timestep (seconds)", type=float, default=0.005)
    arg("--fmax", help="max sim frequency (Hz)", type=float, default=10)
    arg("--kappa", help="", type=float, default=0.045)
    arg("--qfexp", help="Q frequency exponent", type=float, default=0.6)
    # HF IN, line 10
    arg(
        "--rvfac",
        help="rupture velocity factor (rupture : Vs)",
        type=float,
        default=0.8,
    )
    arg("--rvfac_shal", help="rvfac shallow fault multiplier", type=float, default=0.7)
    arg("--rvfac_deep", help="rvfac deep fault multiplier", type=float, default=0.7)
    arg(
        "--czero",
        help="C0 coefficient, < -1 for binary default",
        type=float,
        default=2.1,
    )
    arg(
        "--calpha",
        help="Ca coefficient, < -1 for binary default",
        type=float,
        default=-99,
    )
    # HF IN, line 11
    arg("--mom", help="seismic moment, -1: use rupture model", type=float, default=-1.0)
    arg(
        "--rupv",
        help="rupture velocity, -1: use rupture model",
        type=float,
        default=-1.0,
    )
    # HF IN, line 13
    arg(
        "-m",
        "--velocity-model",
        help="path to velocity model (1D)",
        default=os.path.join(
            shared_defaults.vel_mod_dir, "Mod-1D/Cant1D_v2-midQ_leer.1d"
        ),
    )
    arg("-s", "--site-vm-dir", help="dir containing site specific velocity models (1D)")
    # HF IN, line 14
    arg("--vs-moho", help="depth to moho, < 0 for 999.9", type=float, default=999.9)
    # HF IN, line 17
    arg("--fa_sig1", help="fourier amplitute uncertainty (1)", type=float, default=0.0)
    arg("--fa_sig2", help="fourier amplitude uncertainty (2)", type=float, default=0.0)
    arg("--rv_sig1", help="rupture velocity uncertainty", type=float, default=0.1)
    # HF IN, line 18
    arg(
        "--path_dur",
        help="""path duration model
        0:GP2010 formulation
        1:[DEFAULT] WUS modification trial/error
        2:ENA modification trial/error
        11:WUS formulation of BT2014, overpredicts for multiple rays
        12:ENA formulation of BT2015, overpredicts for multiple rays""",
        type=int,
        default=1,
    )
    try:
        args = parser.parse_args()
    except SystemExit:
        # invalid arguments or -h
        comm.Abort()

if hasattr(args, 'version') and args.version is not None:
    args.sim_bin = binary_version.get_hf_binmod(args.version)

if is_master:
    logger.debug("=" * 50)
    # random seed
    seed_file = os.path.join(os.path.dirname(args.out_file), "SEED")
    if args.seed == -1:
        logger.debug("seed is always randomised.")
    elif os.path.isfile(seed_file):
        args.seed = np.loadtxt(seed_file, dtype="i", ndmin=1)[0]
        logger.debug("seed taken from file: {}".format(args.seed))
    elif args.seed == 0:
        args.seed = random_seed()
        np.savetxt(seed_file, np.array([args.seed], dtype=np.int32), fmt="%i")
        logger.debug("seed generated: {}".format(args.seed))
    else:
        logger.debug("seed from command line: {}".format(args.seed))
    # Logging each argument
    for key in vars(args):
        logger.debug("{} : {}".format(key, getattr(args, key)))

args = comm.bcast(args, root=master)

# if not args.independent:
#     args.seed += rank

mh = MPIFileHandler.MPIFileHandler(
    os.path.join(os.path.dirname(args.out_file), "HF.log")
)
formatter = logging.Formatter("%(asctime)s:%(name)s:%(levelname)s:%(message)s")
mh.setFormatter(formatter)
logger.addHandler(mh)

nt = int(round(args.duration / args.dt))
stations = np.loadtxt(
    args.station_file, ndmin=1, dtype=[("lon", "f4"), ("lat", "f4"), ("name", "|S8")]
)
head_total = HEAD_SIZE + HEAD_STAT * stations.size
block_size = nt * N_COMP * FLOAT_SIZE
file_size = head_total + stations.size * block_size

# initialise output with general metadata
def initialise(check_only=False):
    with open(args.out_file, mode="rb" if check_only else "w+b") as out:
        # int/bool parameters, rayset must be fixed to length = 4
        fwrs = args.rayset + [0] * (4 - len(args.rayset))
        i4 = np.array(
            [
                stations.size,
                nt,
                args.seed,
                not args.no_siteamp,
                args.path_dur,
                len(args.rayset),
                fwrs[0],
                fwrs[1],
                fwrs[2],
                fwrs[3],
                nbu,
                ift,
                nl_skip,
                ic_flag,
                args.seed >= 0,
                args.site_vm_dir != None,
            ],
            dtype="i4",
        )
        # float parameters
        f4 = np.array(
            [
                args.duration,
                args.dt,
                args.t_sec,
                args.sdrop,
                args.kappa,
                args.qfexp,
                args.fmax,
                flo,
                fhi,
                args.rvfac,
                args.rvfac_shal,
                args.rvfac_deep,
                args.czero,
                args.calpha,
                args.mom,
                args.rupv,
                args.vs_moho,
                vp_sig,
                vsh_sig,
                rho_sig,
                qs_sig,
                args.fa_sig1,
                args.fa_sig2,
                args.rv_sig1,
            ],
            dtype="f4",
        )
        # string parameters
        if args.site_vm_dir != None:
            vm = args.site_vm_dir
        else:
            vm = args.velocity_model
        s64 = np.array(list(map(os.path.basename, [args.stoch_file, vm])), dtype="|S64")
        # station metadata
        stat_head = np.zeros(
            stations.size,
            dtype={
                "names": ["lon", "lat", "name"],
                "formats": ["f4", "f4", "|S8"],
                "itemsize": HEAD_STAT,
            },
        )
        for column in stat_head.dtype.names:
            stat_head[column] = stations[column]

        # verify or write
        if check_only:
            assert np.min(np.fromfile(out, dtype=i4.dtype, count=i4.size) == i4)
            assert np.min(np.fromfile(out, dtype=f4.dtype, count=f4.size) == f4)
            assert np.min(np.fromfile(out, dtype=s64.dtype, count=s64.size) == s64)
            out.seek(HEAD_SIZE)
            assert np.min(
                np.fromfile(out, dtype=stat_head.dtype, count=stat_head.size)
                == stat_head
            )
        else:
            i4.tofile(out)
            f4.tofile(out)
            s64.tofile(out)
            out.seek(HEAD_SIZE)
            stat_head.tofile(out)


def unfinished(out_file):
    try:
        with open(out_file, "rb") as hff:
            hff.seek(HEAD_SIZE)
            # checkpoints are vs and e_dist written to file
            # assume continuing machine is the same endian
            checkpoints = (
                np.fromfile(
                    hff,
                    count=stations.size,
                    dtype={
                        "names": ["vs"],
                        "formats": ["f4"],
                        "offsets": [20],
                        "itemsize": HEAD_STAT,
                    },
                )["vs"]
                > 0
            )
    except IOError:
        # file not created yet
        return
    if checkpoints.size < stations.size:
        # file size is too short (simulation not even started properly)
        return
    if os.stat(args.out_file).st_size > file_size:
        # file size is too long (probably different simulation)
        return
    if np.min(checkpoints):
        try:
            logger.debug("Checkpoints found.")
            initialise(check_only=True)
            logger.error("HF Simulation already completed.")
            comm.Abort()
        except AssertionError:
            return
    # seems ok to continue simulation
    return np.invert(checkpoints)


station_mask = None
if is_master:
    station_mask = unfinished(args.out_file)
    if station_mask is None or sum(station_mask) == stations.size:
        logger.debug("No valid checkpoints found. Starting fresh simulation.")
        initialise()
        station_mask = np.ones(stations.size, dtype=np.bool)
    else:
        try:
            initialise(check_only=True)
            logger.info(
                "{} of {} stations completed. Resuming simulation.".format(
                    stations.size - sum(station_mask), stations.size
                )
            )
        except AssertionError:
            logger.warning("Simulation parameters mismatch. Starting fresh simulation.")
            initialise()
            station_mask = np.ones(stations.size, dtype=np.bool)
station_mask = comm.bcast(station_mask, root=master)
stations_todo = stations[station_mask]
stations_todo_idx = np.arange(stations.size)[station_mask]


def run_hf(local_statfile, n_stat, idx_0, velocity_model=args.velocity_model):
    """
    Runs HF Fortran code.
    """
    if args.seed >= 0:
        assert n_stat == 1
        seed = args.seed + idx_0
    else:
        seed = random_seed()

    logger.debug(
        "run_hf({}, {}, {}) seed: {}".format(local_statfile, n_stat, idx_0, seed)
    )
    stdin = "\n".join(
        [
            "",
            str(args.sdrop),
            local_statfile,
            args.out_file,
            "%d %s" % (len(args.rayset), " ".join(map(str, args.rayset))),
            str(int(not args.no_siteamp)),
            "%d %d %s %s" % (nbu, ift, flo, fhi),
            str(seed),
            str(n_stat),
            "%s %s %s %s %s"
            % (args.duration, args.dt, args.fmax, args.kappa, args.qfexp),
            "%s %s %s %s %s"
            % (args.rvfac, args.rvfac_shal, args.rvfac_deep, args.czero, args.calpha),
            "%s %s" % (args.mom, args.rupv),
            args.stoch_file,
            args.velocity_model,
            str(args.vs_moho),
            "%d %s %s %s %s %d" % (nl_skip, vp_sig, vsh_sig, rho_sig, qs_sig, ic_flag),
            velocity_name,
            "%s %s %s" % (args.fa_sig1, args.fa_sig2, args.rv_sig1),
            str(args.path_dur),
            str(head_total + idx_0 * (nt * N_COMP * FLOAT_SIZE)),
            "",
        ]
    )

    # run HF binary
    p = Popen([args.sim_bin], stdin=PIPE, stderr=PIPE, universal_newlines=True)
    stderr = p.communicate(stdin)[1]

    # load vs
    with open(velocity_model, "r") as vm:
        vm.readline()
        vs = np.float32(float(vm.readline().split()[2]) * 1000.0)

    # e_dist is the only other variable that HF calculates
    e_dist = np.fromstring(stderr, dtype="f4", sep="\n")
    try:
        assert e_dist.size == n_stat
    except AssertionError:
        logger.error("Expected {} e_dist values, got {}".format(n_stat, e_dist.size))
        logger.error("Dumping Fortran stderr to hf_err_{}".format(idx_0))

        with open("hf_err_%d" % (idx_0), "w") as e:
            e.write(stderr)
        comm.Abort()

    # write e_dist and vs to file
    with open(args.out_file, "r+b") as out:
        out.seek(HEAD_SIZE + idx_0 * HEAD_STAT)
        for i in range(n_stat):
            out.seek(HEAD_STAT - 2 * FLOAT_SIZE, 1)
            e_dist[i].tofile(out)
            vs.tofile(out)


def validate_end(idx_n):
    """
    Verify filesize has been extended by the correct amount.
    idx_n: position (starting at 1) of last station to be completed
    """
    try:
        assert os.stat(args.out_file).st_size == head_total + idx_n * block_size
    except AssertionError:
        msg = "Expected size: %d bytes (last stat idx: %d), actual %d bytes." % (
            head_total + idx_n * block_size,
            idx_n,
            os.stat(args.out_file).st_size,
        )
        # this is here because kupe fails at stdio
        with open("hf_err_validate", "w") as e:
            e.write(msg)
        logger.error("Validation failed: {}".format(msg))
        comm.Abort()


# distribute work, must be sequential for optimisation,
# and for validation function above to be thread safe
d = stations_todo.size // size
r = stations_todo.size % size
start = rank * d + min(r, rank)
work = stations_todo[start : start + d + (rank < r)]
work_idx = stations_todo_idx[start : start + d + (rank < r)]

# process data to give Fortran code
t0 = MPI.Wtime()
in_stats = mkstemp()[1]
if args.seed >= 0:
    vm = args.velocity_model
    for s in range(work.size):
        if args.site_vm_dir != None:
            vm = os.path.join(args.site_vm_dir, "%s.1d" % (stations_todo[s]["name"]))
        np.savetxt(in_stats, work[s : s + 1], fmt="%f %f %s")
        run_hf(in_stats, 1, work_idx[s], velocity_model=vm)
        if rank == size - 1:
            validate_end(work_idx[s] + 1)
elif args.seed == -1:
    # have to be careful if checkpointing, work is not always consecutive
    c0 = 0
    for c_work_idx in np.split(work_idx, np.where(np.diff(work_idx) != 1)[0] + 1):
        for s in range(0, c_work_idx.size, MAX_STATLIST):
            n_stat = min(MAX_STATLIST, c_work_idx.size - s)
            np.savetxt(in_stats, work[c0 + s : c0 + s + n_stat], fmt="%f %f %s")
            run_hf(in_stats, n_stat, c_work_idx[s])
        c0 += c_work_idx.size
        if rank == size - 1:
            validate_end(work_idx[c0 - 1] + 1)
os.remove(in_stats)
print("Process %03d of %03d finished (%.2fs)." % (rank, size, MPI.Wtime() - t0))
comm.Barrier() #all ranks wait here until rank 0 arrives to announce all completed
if is_master:
    logger.debug("Simulation completed.")
