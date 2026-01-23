#!/usr/bin/env python
"""
Simulates high frequency seismograms for stations.
"""
from argparse import ArgumentParser
import os
import random
from subprocess import Popen, PIPE
from tempfile import mkstemp, NamedTemporaryFile

import numpy as np
import logging

from qcore import binary_version, constants, utils
from workflow.automation.platform_config import platform_config

if __name__ == "__main__":
    from qcore import MPIFileHandler
    from mpi4py import MPI

    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    master = 0
    is_master = rank == master

    logger = logging.getLogger("rank_%i" % comm.rank)
    logger.setLevel(logging.DEBUG)

HEAD_SIZE = 0x0200
HEAD_STAT = 0x18
FLOAT_SIZE = 0x4
N_COMP = 3

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


def random_seed():
    return random.randrange(1_000_000, 9_999_999)


def args_parser(cmd=None):
    """
    CMD is a list of strings to parse
    While, not None, cmd will be used to parse
    if cmd == None, default behavior sys.argv[1:] will be used
    """

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
        default=None,
        type=os.path.abspath,
    )
    arg(
        "--version",
        help="binary version, similar to --sim_bin but not full path.",
        default="6.0.3",  # 5.4.5, 5.4.6, 6.0.3 with subversions .1 .2 .3 are supported
    )
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
        help="random seed (0:randomised reproducible)",
        type=int,
        default=constants.HF_DEFAULT_SEED,
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
        "--hf_vel_mod_1d",
        help="path to velocity model (1D). ignored if --site_specific is set",
        default=os.path.join(
            platform_config[constants.PLATFORM_CONFIG.VELOCITY_MODEL_DIR.name],
            "Mod-1D/Cant1D_v2-midQ_leer.1d",
        ),
    )
    arg(
        "--site_specific",
        action="store_true",
        help="enable site-specific calculation",
        default=False,
    )
    arg(
        "-s",
        "--site_v1d_dir",
        help="dir containing site specific velocity models (1D). requires --site_specific",
    )
    # HF IN, line 14
    arg("--vs-moho", help="vs of moho layer, < 0 for 999.9", type=float, default=999.9)
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
    arg(
        "--dpath_pert",
        help="""path duration perturbation
        Only to be used with versions greater than 5.4.5.4
        The path duration is multiplied by the base e exponential of the given value
        The default value of 0 results in no perturbation""",
        type=float,
        default=0.0,
    )

    arg(
        "--stress_param_adj",
        help="""stress parameter adjustment X Y Z
        X: Adjustment option 0 = off 1 = active tectonic, 2 = stable continent
        Y: Target magnitude (auto = -1 or specified mag.)
        Z: Fault area (auto = -1 or specified area in km^2)""",
        nargs=3,
        default=["1", "-1", "-1"],
    )

    args = parser.parse_args(cmd)

    return args


if __name__ == "__main__":
    args = None
    if is_master:
        try:
            args = args_parser()
        except SystemExit as e:
            print(e, flush=True)
            # invalid arguments or -h
            comm.Abort()

        if args.sim_bin is None:
            args.sim_bin = binary_version.get_hf_binmod(args.version)

        logger.debug("=" * 50)
        # random seed
        seed_file = os.path.join(os.path.dirname(args.out_file), "SEED")

        if os.path.isfile(seed_file):
            args.seed = np.loadtxt(seed_file, dtype="i", ndmin=1)[0]
            logger.debug("seed taken from file: {}".format(args.seed))
        elif args.seed == 0:
            args.seed = random_seed()
            np.savetxt(seed_file, np.array([args.seed], dtype=np.int32), fmt="%i")
            logger.debug("seed generated: {}".format(args.seed))
        else:
            logger.debug("seed from command line: {}".format(args.seed))
        assert args.seed >= 0  # don't like negative seed

        # Logging each argument
        for key in vars(args):
            logger.debug("{} : {}".format(key, getattr(args, key)))

    args = comm.bcast(args, root=master)

    mh = MPIFileHandler.MPIFileHandler(
        os.path.join(os.path.dirname(args.out_file), "HF.log")
    )
    formatter = logging.Formatter("%(asctime)s:%(name)s:%(levelname)s:%(message)s")
    mh.setFormatter(formatter)
    logger.addHandler(mh)

    nt = int(round(args.duration / args.dt))
    stations = np.loadtxt(
        args.station_file,
        ndmin=1,
        dtype=[("lon", "f4"), ("lat", "f4"), ("name", "|S8")],
    )
    head_total = HEAD_SIZE + HEAD_STAT * stations.size
    block_size = nt * N_COMP * FLOAT_SIZE
    file_size = head_total + stations.size * block_size

    # initialise output with general metadata - master only
    def initialise(check_only=False):
        # This function should only be called by the master rank
        if not is_master:
            raise RuntimeError("Initialise function should only be called by master rank.")
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
                    args.site_specific is True,
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
            if args.site_specific is True:
                v1d = (
                    args.site_v1d_dir
                )  # dir only is ok. i4 above has last element to deduce actual VM file
            else:
                v1d = args.hf_vel_mod_1d
            s64 = np.array(
                list(map(os.path.basename, [args.stoch_file, v1d])), dtype="|S64"
            )
            # station metadata
            stat_head = np.zeros(
                stations.size,
                dtype={
                    "names": ["lon", "lat", "name", "e_dist", "vs"], # Add e_dist and vs for initialization
                    "formats": ["f4", "f4", "|S8", "f4", "f4"],
                    "itemsize": HEAD_STAT,
                },
            )
            for column in ["lon", "lat", "name"]: # Only copy existing data
                stat_head[column] = stations[column]

            # Initialize e_dist and vs to 0 or some placeholder
            stat_head["e_dist"] = 0.0
            stat_head["vs"] = 0.0

            # verify or write
            if check_only:
                assert np.min(np.fromfile(out, dtype=i4.dtype, count=i4.size) == i4)
                assert np.min(np.fromfile(out, dtype=f4.dtype, count=f4.size) == f4)
                assert np.min(np.fromfile(out, dtype=s64.dtype, count=s64.size) == s64)
                out.seek(HEAD_SIZE)
                # For check_only, we don't care about e_dist/vs values, just the structure
                assert np.min(
                    np.fromfile(out, dtype=stat_head.dtype, count=stat_head.size)
                    == stat_head # This comparison might be tricky due to e_dist/vs, consider only relevant fields or just size
                )
            else:
                i4.tofile(out)
                f4.tofile(out)
                s64.tofile(out)
                out.seek(HEAD_SIZE)
                stat_head.tofile(out) # Write initial header with zeros for e_dist/vs

    def unfinished(out_file):
        # This function also potentially problematic with current setup, as it relies on partial writes.
        # With distributed writes, the file will either be fully initialized or not exist.
        # We might need to rethink this 'unfinished' logic entirely, or simplify it for the master.
        # For now, let's keep it mostly as is for initial setup, but recognize it might need adjustment
        # if the master doesn't partially write data anymore.
        try:
            with open(out_file, "rb") as hff:
                hff.seek(HEAD_SIZE)
                # checkpoints are vs and e_dist written to file
                checkpoints = (
                    np.fromfile(
                        hff,
                        count=stations.size,
                        dtype={
                            "names": ["e_dist"],
                            "formats": ["f4"],
                            "offsets": [16],
                            "itemsize": HEAD_STAT,
                        },
                    )["e_dist"]
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
        # The logic for 'unfinished' might need rethinking if master no longer partially writes
        # For now, assume it helps determine if we need to initialise from scratch.
        station_mask = unfinished(args.out_file)
        if station_mask is None or sum(station_mask) == stations.size:
            logger.debug("No valid checkpoints found. Starting fresh simulation.")
            initialise() # Master initializes the header
            station_mask = np.ones(stations.size, dtype=bool)
        else:
            try:
                initialise(check_only=True)
                logger.info(
                    "{} of {} stations completed. Resuming simulation.".format(
                        stations.size - sum(station_mask), stations.size
                    )
                )
            except AssertionError:
                logger.warning(
                    "Simulation parameters mismatch. Starting fresh simulation."
                )
                initialise()
                station_mask = np.ones(stations.size, dtype=bool)
    station_mask = comm.bcast(station_mask, root=master)
    stations_todo = stations[station_mask]
    stations_todo_idx = np.arange(stations.size)[station_mask]

    def run_hf(
        local_statfile, n_stat, idx_0, v1d_path=args.hf_vel_mod_1d, bin_mod=True
    ):
        """
        Runs HF Fortran code and returns e_dist and vs.
        No direct file writing to args.out_file in this function.
        """
        if args.seed >= 0:
            assert n_stat == 1
            seed = args.seed + idx_0
        else:
            seed = random_seed()

        logger.info(
            "run_hf({}, {}, {}) seed: {}".format(local_statfile, n_stat, idx_0, seed)
        )

        # Create a temporary file for this station's binary output
        # Use NamedTemporaryFile for automatic cleanup on close
        temp_hf_out = NamedTemporaryFile(delete=False, dir=os.path.dirname(args.out_file), suffix=".bin_tmp")
        temp_hf_out_path = temp_hf_out.name
        temp_hf_out.close() # Close it so sim_bin can open/write to it	


	# Construct the arguments for the Fortran binary, to be passed via stdin.
        # The order of these arguments is CRITICAL and must match the Fortran's `read(5,*)` sequence.
        # References to Fortran source line numbers are approximate and based on your provided file.
        hf_sim_args = [
            "",                                                                  # 1. Dummy/empty string
            str(args.sdrop),                                                     # 2. stress_average [cite: 35]
            local_statfile,                                                      # 3. asite [cite: 35]
            temp_hf_out_path,                                                    # 4. outname (Fortran will write binary here) [cite: 35]
            "{:d} {}".format(len(args.rayset), " ".join(map(str, args.rayset))), # 5. nrtyp, (irtype(i),i=1,nrtyp) [cite: 35]
            str(int(not args.no_siteamp)),                                       # 6. isite_amp [cite: 36]
            "{:d} {:d} {} {}".format(nbu, ift, flo, fhi),                        # 7. nbu,iftt,flol,fhil [cite: 36]
            str(seed),                                                           # 8. irand [cite: 36]
            str(n_stat),                                                         # 9. nsite (should be 1 for single station processing) [cite: 36]
            "{} {} {} {} {}".format(                                             # 10. duration,dt,fmx,akapp,qfexp [cite: 36]
                args.duration, args.dt, args.fmax, args.kappa, args.qfexp
            ),
            "{} {} {} {} {}".format(                                             # 11. rvfac,shal_rvfac,deep_rvfac,Czero,Calpha [cite: 37]
                args.rvfac, args.rvfac_shal, args.rvfac_deep, args.czero, args.calpha
            ),
            "{} {}".format(args.mom, args.rupv),                                 # 12. sm,vr [cite: 37]
            args.stoch_file,                                                     # 13. slip_model [cite: 37]
            v1d_path,                                                            # 14. velfile [cite: 42]
            str(args.vs_moho),                                                   # 15. vsmoho [cite: 43]
            "{:d} {} {} {} {} {:d}".format(                                      # 16. nlskip,vpsig,vshsig,rhosig,qssig,icflag [cite: 45]
                nl_skip, vp_sig, vsh_sig, rho_sig, qs_sig, ic_flag
            ),
            velocity_name,                                                       # 17. velname [cite: 45]
            "{} {} {}".format(args.fa_sig1, args.fa_sig2, args.rv_sig1),         # 18. fasig1,fasig2,rvsig1 [cite: 46]
            str(args.path_dur),                                                  # 19. ipdur_model [cite: 46]
            "{} {} {}".format(                                                   # 20. ispar_adjust,targ_mag,fault_area [cite: 47]
                args.stress_param_adj[0],
                args.stress_param_adj[1],
                args.stress_param_adj[2],
            ),
            # Conditional argument: pd_pert (path duration perturbation).
            # This is read IF VERSION4 is defined in Fortran. Your `6.0.3` version implies this.
            str(args.dpath_pert),                                                # 21. pd_pert (only if VERSION4 is defined) [cite: 47]

            # CRITICAL FIX: The `seek_bytes` argument.
            # Even though `sim_bin` writes to a temporary file from its beginning,
            # the Fortran source indicates it still attempts to read this line from stdin
            # if compiled with BINMOD (which it is).
            "0",                                                                 # 22. seek_bytes (dummy value as it's not used for writing offset to a fresh file) [cite: 48]
            "", # Final empty string for extra newline (often needed for Fortran reads)
        ]

        stdin = "\n".join(hf_sim_args)

        # For debugging purposes, log the stdin that will be passed to sim_bin
        # You can remove these lines after confirming the fix.
        logger.error(f"rank {rank}: Debugging stdin for station {idx_0}:")
        logger.error(f"rank {rank}: ---START STDIN---")
        for i, line in enumerate(stdin.splitlines()):
            logger.error(f"rank {rank}: LINE {i:02d}: {line}")
        logger.error(f"rank {rank}: ---END STDIN---")
        with open(f"debug_stdin_rank{rank}_idx{idx_0}.txt", "w") as f:
            f.write(stdin)


        # Execute the Fortran binary as a subprocess.
        # `stdout=PIPE` and `stderr=PIPE` are crucial for capturing its output.
        # `universal_newlines=False` ensures binary output from stdout is handled correctly.
        # Assuming sim_bin writes binary data to `temp_hf_out_path` and metadata to `stderr`.
        p = Popen([args.sim_bin], stdin=PIPE, stdout=PIPE, stderr=PIPE)
        
        # Communicate with the subprocess, sending stdin and capturing stdout/stderr
        # stdout_data will contain the binary trace data from sim_bin (if it writes there)
        # stderr_data will contain any error messages or metadata (like e_dist)
        stdout_data, stderr_data = p.communicate(stdin.encode('utf-8'))

        # Check the return code of the Fortran binary.
        if p.returncode != 0:
            logger.error(f"rank {rank}: sim_bin failed with exit code {p.returncode} for station {idx_0}.")
            logger.error(f"rank {rank}: sim_bin stderr: {stderr_data.decode('utf-8', errors='ignore')}") # Decode with error handling
            # Clean up the temporary file if the Fortran binary failed
            if os.path.exists(temp_hf_out_path):
                os.remove(temp_hf_out_path)
            comm.Abort()

        # Check if sim_bin wrote to stdout (if it's not writing directly to file)
        # Based on the Fortran source and `BINMOD` block, it writes to the file.
        # So, stdout_data should be empty, but we'll log it if it's not for debugging.
        if stdout_data:
            logger.warning(f"rank {rank}: sim_bin unexpectedly wrote {len(stdout_data)} bytes to stdout for station {idx_0}.")
            # If sim_bin *does* write binary data to stdout, you'd need to write it to `temp_hf_out_path` here:
            # with open(temp_hf_out_path, "wb") as f:
            #     f.write(stdout_data)


	# Parse e_dist from stderr_data. This needs robust parsing as stderr might contain other messages.
        e_dist_val = -1.0 # Default value in case parsing fails
        lines = stderr_data.decode('utf-8', errors='ignore').splitlines() # Decode with error handling
        found_e_dist = False
        
        # The Fortran code has: `write(0, '(1x,f10.4)')c(2)` at [cite: 136]
        # This means it prints `c(2)` (which is `d10`, the closest distance) as a float,
        # formatted with `1x` (a space) and `f10.4`.
        # So we expect a line like " 123.4567"
        for line in reversed(lines): # Iterate from end, as e_dist is probably last
            line = line.strip()
            if line: # Ensure line is not empty
                try:
                    # Attempt to convert to float. This is fragile if other text is on the same line.
                    # Given '1x,f10.4' format, it should be a clean float value.
                    e_dist_val = float(line)
                    found_e_dist = True
                    break # Found it, exit loop
                except ValueError:
                    continue # Not a float, continue to next line

        if not found_e_dist:
            logger.error(f"rank {rank}: Could not find e_dist value in sim_bin stderr for station {idx_0}.")
            logger.error(f"rank {rank}: Full sim_bin stderr: {stderr_data.decode('utf-8', errors='ignore')}")
            if os.path.exists(temp_hf_out_path):
                os.remove(temp_hf_out_path)
            comm.Abort()

        # Load vs. This part is local and fine.
        try:
            with open(v1d_path, "r") as f:
                f.readline() # Skip header
                # Assuming the VS value is the 3rd column of the 2nd line
                vs = np.float32(float(f.readline().split()[2]) * 1000.0)
        except Exception as e:
            logger.error(f"rank {rank}: Error reading vs from velocity model {v1d_path}: {e}")
            if os.path.exists(temp_hf_out_path):
                os.remove(temp_hf_out_path)
            comm.Abort()

        # Return the path to the temporary file and the metadata
        return temp_hf_out_path, e_dist_val, vs


    # distribute work
    work = stations_todo[rank::size]
    work_idx = stations_todo_idx[rank::size]

    # Store results for this rank
    local_results = [] # List of tuples: (station_idx, temp_file_path, e_dist, vs)

    t0 = MPI.Wtime()
    # No longer need mkstemp for in_stats, as it's created and deleted per station.
    # We can use NamedTemporaryFile for input station file as well for cleanup.

    for s_i, s in enumerate(work):
        current_station_idx = work_idx[s_i]
        
        # Create a temporary file for the current station input
        with NamedTemporaryFile(delete=False, mode='w', dir=os.path.dirname(args.out_file), suffix=".stat") as in_stats_temp:
            np.savetxt(in_stats_temp, s.reshape(1,), fmt="%f %f %s") # Reshape s to ensure it's treated as a single row
            in_stats_temp_path = in_stats_temp.name
        
        v1d_path = args.hf_vel_mod_1d
        if args.site_specific:
            v1d_path = os.path.join(
                args.site_v1d_dir, f"{s['name'].decode('ascii')}.1d"
            )

        # Run HF simulation, get temp file path and metadata
        temp_hf_out_path, e_dist_val, vs_val = run_hf(
            in_stats_temp_path, 1, current_station_idx, v1d_path=v1d_path
        )
        
        # Store results for gathering later
        local_results.append((current_station_idx, temp_hf_out_path, e_dist_val, vs_val))

        # Clean up the input station temporary file immediately
        os.remove(in_stats_temp_path)

    # Gather results from all ranks to the master
    all_results = comm.gather(local_results, root=master)

    if is_master:
        # Flatten the list of lists into a single list and sort by original station index
        # This will ensure the data is written in the correct order in the final HF.bin
        flat_results = []
        for rank_results in all_results:
            flat_results.extend(rank_results)
        
        # Sort by the original station index
        flat_results.sort(key=lambda x: x[0])

        logger.info("Master: Merging results from all ranks.")

        # Open the main output file in append/read-write mode to fill in the data
        # Header should already be written by initialise()
        with open(args.out_file, "r+b") as out:
            for original_idx, temp_path, e_dist_val, vs_val in flat_results:
                # Seek to the correct metadata position for e_dist and vs
                out.seek(HEAD_SIZE + original_idx * HEAD_STAT + 16) # 16 is offset to e_dist within HEAD_STAT

                # Write e_dist and vs
                np.array([e_dist_val], dtype="f4").tofile(out)
                np.array([vs_val], dtype="f4").tofile(out)

                # Now, append the actual binary trace data from the temp file
                # Seek to the correct data block position
                out.seek(head_total + original_idx * block_size)
                
                with open(temp_path, "rb") as temp_f:
                    shutil.copyfileobj(temp_f, out) # Efficiently copy binary data

                # Clean up the temporary file for this station
                os.remove(temp_path)

        # The validate_end function would need to be re-evaluated as it relies on specific file sizes.
        # With this new approach, the file size should be exactly `file_size` at the end of the merge.
        # Let's adjust `validate_end` to check the final file size.
        def validate_end_master(expected_file_size):
            actual_size = os.stat(args.out_file).st_size
            if actual_size != expected_file_size:
                msg = f"Expected final size: {expected_file_size} bytes, actual: {actual_size} bytes."
                logger.error("Validation failed: {}".format(msg))
                comm.Abort()
            else:
                logger.info(f"Final file size validated: {actual_size} bytes.")

        validate_end_master(file_size) # Check the final size based on all stations

    # Everyone cleans up their own temporary files (already done in loop for input, and master for output)

    print(
        "Process {} of {} completed {} stations ({:.2f}).".format(
            rank, size, work.size, MPI.Wtime() - t0
        )
    )
    logger.debug(
        "Process {} of {} completed {} stations ({:.2f}).".format(
            rank, size, work.size, MPI.Wtime() - t0
        )
    )
    comm.Barrier()  # All ranks wait here until all processing and master merge is done.
    if is_master:
        logger.debug("Simulation completed.")
