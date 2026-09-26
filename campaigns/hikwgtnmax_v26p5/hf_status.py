"""Read-only status of the HikWgtnmax HF array, with a full check of each
finished output.

For every line of the commands file (median first, then REL01-REL50) it shows:
  - the task's Slurm state, from the jobs logged in submissions.txt next to
    the commands file (a later job overrides an earlier one for the same task,
    so a resubmitted task shows its latest run);
  - how many stations are done: hf_sim.py marks a station done by writing its
    e_dist (> 0) into the station's header record, and resumes from these;
  - for a running task, its rate so far and the time left at that rate.

Once HF.log says "Simulation completed", the HF.bin is checked in full:
  - the header against the command (seed, stoch file, settings) and the
    campaign's choices (Cant1D_v3-midQ_OneRay.1d, rayset 1);
  - the header fields that come from hf_sim.py's defaults, against an HF.bin
    from Sung's v26p6 runs;
  - the station table against the station list, with every station done and
    carrying the 1D model's surface Vs;
  - the exact size, and the data of a sample of stations: finite, with every
    component non-zero.
It exits 1 if a task's Slurm state is anything but pending, running or
completed, or if a finished output fails a check.

Usage: python hf_status.py COMMANDS_FILE
It needs NeSI's sacct and filesystems but writes nothing, so it can be piped
over ssh from a checkout (see the README).
"""

import functools
import re
import shlex
import subprocess
import sys
from collections import Counter
from pathlib import Path

import numpy as np

MODEL = "/nesi/project/nesi00213/VelocityModel/Mod-1D/Cant1D_v3-midQ_OneRay.1d"
RAYSET = [1]
# Sung's v26p6 HF output (leer, rayset 1), for the fields from hf_sim.py's defaults
REFERENCE = "/nesi/nobackup/nesi00213/RunFolder/Cybershake/v26p6/Runs/PalliserKai/PalliserKai_REL01/HF/Acc/HF.bin"
N_SAMPLE = 64  # stations whose data are checked in each finished output
LABELS = ["finished and checked OK", "with problems", "running", "pending", "not submitted"]

# qcore HFSeis layout: 16 int32, 24 float32, then the stoch file and 1D model
# names (64 bytes each) in a 0x200 header, then a 0x18 record per station
HEAD_SIZE, HEAD_STAT = 0x200, 0x18
I4 = ["nstat", "nt", "seed", "siteamp", "path_dur", "nrayset", "rayset1", "rayset2", "rayset3",
      "rayset4", "nbu", "ift", "nl_skip", "ic_flag", "seed_given", "site_specific"]
F4 = ["duration", "dt", "t_sec", "sdrop", "kappa", "qfexp", "fmax", "flo", "fhi", "rvfac",
      "rvfac_shal", "rvfac_deep", "czero", "calpha", "mom", "rupv", "vs_moho", "vp_sig", "vsh_sig",
      "rho_sig", "qs_sig", "fa_sig1", "fa_sig2", "rv_sig1"]
STAT = np.dtype({"names": ["lon", "lat", "name", "e_dist", "vs"],
                 "formats": ["f4", "f4", "S8", "f4", "f4"],
                 "offsets": [0, 4, 8, 16, 20], "itemsize": HEAD_STAT})


def read_header(path, nstat):
    """The header fields, and the first nstat station records."""
    with open(path, "rb") as f:
        head = dict(zip(I4, np.fromfile(f, "i4", len(I4)).tolist()))
        head.update(zip(F4, np.fromfile(f, "f4", len(F4)).tolist()))
        head["stoch"], head["model"] = (s.decode() for s in np.fromfile(f, "S64", 2))
        f.seek(HEAD_SIZE)
        return head, np.fromfile(f, STAT, nstat)


@functools.lru_cache
def read_stations(statlist):
    # as hf_sim.py reads it
    return np.loadtxt(statlist, ndmin=1, dtype=[("lon", "f4"), ("lat", "f4"), ("name", "|S8")])


def surface_vs(model):
    """The Vs hf_sim.py records for every station: the 1D model's first layer, in m/s."""
    with open(model) as f:
        f.readline()
        return np.float32(float(f.readline().split()[2]) * 1000.0)


def log_completed(log):
    """Whether the end of HF.log has hf_sim.py's final message."""
    if not log.exists():
        return False
    with open(log, "rb") as f:
        f.seek(max(0, log.stat().st_size - 65536))
        return b"Simulation completed" in f.read()


def task_ids(jobid):
    """The array task ids in a sacct JobID such as 9325809_7 or 9325809_[25-50%28]."""
    ids = []
    for part in jobid.split("_", 1)[1].strip("[]").split("%")[0].split(","):
        first, _, last = part.partition("-")
        ids.extend(range(int(first), int(last or first) + 1))
    return ids


def seconds(elapsed):
    days, _, hms = elapsed.rpartition("-")
    h, m, s = (int(x) for x in hms.split(":"))
    return int(days or 0) * 86400 + h * 3600 + m * 60 + s


def hm(sec):
    return f"{int(sec // 3600)}:{int(sec % 3600 // 60):02d}"


def slurm_states(submissions):
    """The logged job ids, and task id -> (state, elapsed seconds, node, partition)."""
    jobs = re.findall(r"\bjob (\d+):", submissions.read_text())
    if not jobs:
        sys.exit(f"no jobs logged in {submissions}")
    out = subprocess.run(["sacct", "-j", ",".join(jobs), "-X", "-n", "-P",
                          "-o", "JobID,State,Elapsed,NodeList,Partition"],
                         capture_output=True, text=True, check=True).stdout
    rows = sorted((row.split("|") for row in out.splitlines()),
                  key=lambda row: jobs.index(row[0].split("_")[0]))
    states = {}
    for jobid, state, elapsed, node, partition in rows:
        state = state.split()[0]
        if state == "PENDING":
            node = partition = ""
        for task in task_ids(jobid):
            states[task] = (state, seconds(elapsed), node, partition)
    return jobs, states


def check_output(out, opts, head, stats, stations, reference, vs):
    """Problems found in a finished HF.bin, and a summary of what was checked."""
    nt = int(round(float(opts["--duration"]) / float(opts["--dt"])))
    expected = {"nstat": stations.size, "nt": nt, "seed": int(opts["--seed"]), "siteamp": 1,
                "path_dur": int(opts["--path_dur"]), "nrayset": len(RAYSET), "seed_given": 1,
                "site_specific": 0, "stoch": Path(opts["--slip"]).name, "model": Path(MODEL).name}
    expected.update(zip(["rayset1", "rayset2", "rayset3", "rayset4"], RAYSET + [0] * (4 - len(RAYSET))))
    for key in ("duration", "dt", "sdrop", "kappa", "rvfac"):
        expected[key] = float(np.float32(opts[f"--{key}"]))
    problems = [f"{key} {head[key]!r}, expected {value!r}"
                for key, value in expected.items() if head[key] != value]
    problems += [f"{key} {head[key]!r}, v26p6 reference has {reference[key]!r}"
                 for key in I4 + F4 if key not in expected and head[key] != reference[key]]
    for key in ("lon", "lat", "name"):
        if not np.array_equal(stats[key], stations[key]):
            problems.append(f"station {key}s differ from the station list")
    if (stats["e_dist"] <= 0).any():
        problems.append(f"{(stats['e_dist'] <= 0).sum()} stations not done")
    if (stats["vs"] != vs).any():
        problems.append(f"{(stats['vs'] != vs).sum()} stations with vs other than {vs:g}")
    size = HEAD_SIZE + stations.size * (HEAD_STAT + nt * 3 * 4)
    if out.stat().st_size != size:
        problems.append(f"size {out.stat().st_size}, expected {size}")
        return problems, "data not sampled"
    rng = np.random.default_rng(int(opts["--seed"]))
    sample = np.unique(np.concatenate([np.linspace(0, stations.size - 1, N_SAMPLE // 2).astype(int),
                                       rng.choice(stations.size, N_SAMPLE // 2, replace=False)]))
    peaks = []
    with open(out, "rb") as f:
        for i in sample:
            f.seek(HEAD_SIZE + stations.size * HEAD_STAT + int(i) * nt * 3 * 4)
            acc = np.fromfile(f, "f4", nt * 3).reshape(nt, 3)
            if not np.isfinite(acc).all():
                problems.append(f"station {i} has non-finite values")
            elif not (np.abs(acc).max(axis=0) > 0).all():
                problems.append(f"station {i} has an all-zero component")
            else:
                peaks.append(np.abs(acc).max())
    summary = (f"e_dist {stats['e_dist'].min():.1f}-{stats['e_dist'].max():.1f} km; {len(sample)} sampled stations, "
               f"peak |acc| {min(peaks, default=0):.2f}-{max(peaks, default=0):.1f} cm/s^2")
    return problems, summary


def main():
    commands = Path(sys.argv[1])
    lines = [ln for ln in commands.read_text().splitlines() if ln.strip()]
    jobs, states = slurm_states(commands.parent / "submissions.txt")
    reference = read_header(REFERENCE, 0)[0]
    vs = surface_vs(MODEL)
    print(f"jobs {', '.join(jobs)}; hf_sim.py defaults compared with {REFERENCE}\n")
    print(f"{'task':>4} {'realisation':<18} {'state':<10} {'node':<6} {'partition':<9} {'elapsed':>7} "
          f"{'stations done':>20} {'rate/h':>6} {'left':>5}")
    counts = Counter()
    for task, line in enumerate(lines):
        tokens = shlex.split(line)
        stations, out = read_stations(tokens[5]), Path(tokens[6])
        opts = dict(zip(tokens[7::2], tokens[8::2]))
        state, elapsed, node, partition = states.get(task, ("-", 0, "", ""))
        done = 0
        if out.exists() and out.stat().st_size >= HEAD_SIZE + stations.size * HEAD_STAT:
            head, stats = read_header(out, stations.size)
            done = int((stats["e_dist"] > 0).sum())
        rate = left = ""
        if state == "RUNNING" and elapsed > 1800 and done:
            # assumes the task started from scratch: a resumed task looks faster
            per_sec = done / elapsed
            rate, left = f"{per_sec * 3600:.0f}", hm((stations.size - done) / per_sec)
        print(f"{task:>4} {out.parents[2].name:<18} {state:<10} {node:<6} {partition:<9} "
              f"{hm(elapsed) if elapsed else '':>7} {done:>5}/{stations.size} ({100 * done / stations.size:5.1f}%) "
              f"{rate:>6} {left:>5}")

        problems = []
        if state not in ("PENDING", "RUNNING", "COMPLETED"):
            problems.append(f"Slurm state {state}")
        finished = done and log_completed(out.parent / "HF.log")
        if finished:
            found, summary = check_output(out, opts, head, stats, stations, reference, vs)
            problems += found
            print(f"{'':6}full check {'OK' if not found else 'FAILED'}: {summary}")
        elif state == "COMPLETED":
            problems.append("COMPLETED, but HF.log does not say the simulation completed")
        for problem in problems:
            print(f"{'':6}FAIL: {problem}")
        counts["with problems" if problems else "finished and checked OK" if finished
               else {"RUNNING": "running", "PENDING": "pending"}.get(state, "not submitted")] += 1

    print(f"\n{len(lines)} tasks: " + ", ".join(f"{counts[label]} {label}" for label in LABELS
                                               if counts[label] or label == "with problems"))
    sys.exit(1 if counts["with problems"] else 0)


if __name__ == "__main__":
    main()
